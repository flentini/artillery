/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
const k8s = require('@kubernetes/client-node');
const { Command, flags } = require('@oclif/command');

const {
  readScript,
  parseScript,
  addOverrides,
  // addVariables,
  // resolveConfigTemplates,
  checkConfig,
} = require('../../util');
const debug = require('debug')('commands:kube-run');
const _ = require('lodash');
const fs = require('fs');
const net = require('net');
const Redis = require('ioredis');

class KubeRunCommand extends Command {
  async run() {
    const { flags, argv } = this.parse(KubeRunCommand);

    const inputFiles = argv.concat(flags.input || [], flags.config || []);

    try {
      const script = await prepareTestExecutionPlan(inputFiles, flags);
      this.kubeConfig = new k8s.KubeConfig();
      //kubeConfig.loadFromCluster();
      this.kubeConfig.loadFromDefault();

      console.log('creating redis dependencies...');
      await createRedisService(this.kubeConfig);

      console.log('creating configmap...');
      await createConfigMap(this.kubeConfig, script);

      // TODO wait until the redis pod is up
      // TODO check for something better than port forwarding
      console.log('creating port forwarding...');

      const portForwardDetails = await this.createPortForward(this.kubeConfig, {
        podName: 'artillery-redis-master',
        podPort: 6379,
      });

      // console.log('connectig to redis...');
      this.redis = await waitAndConnectToRedis(
        `redis://0.0.0.0:${portForwardDetails.port}`
      );

      this.listen();

      console.log('creating k8s job...');

      await this.createK8SJob(parseInt(flags.count, 10));

      // await gracefulShutdown();
    } catch (err) {
      console.log('kube-run error:', err);

      throw err;
    } finally {
      // console.log('shutting down redis');

      // this.forwardServer.close();
      // this.redis.shutdown();

      // process.exit(0);
    }
  }

  async createPortForward(kubeConfig, options = {}, namespace = 'default') {
    const p = new k8s.PortForward(kubeConfig);

    this.forwardServer = net
      .createServer((socket) => {
        p.portForward(
          namespace,
          options.podName,
          [options.podPort],
          socket,
          null,
          socket
        );
      })
      .listen(0);

    return this.forwardServer.address();
  }

  async createK8SJob(count = 1, namespace = 'default') {
    try {
      const batchV1Api = this.kubeConfig.makeApiClient(k8s.BatchV1Api);

      return batchV1Api.createNamespacedJob(namespace, {
        apiVersion: 'batch/v1',
        kind: 'Job',
        metadata: {
          name: 'artillery-worker',
          labels: {
            app: 'artillery-worker',
          },
        },
        spec: {
          completions: count,
          parallelism: count,
          template: {
            spec: {
              containers: [
                {
                  name: 'artillery-worker',
                  // image: 'artilleryio/artillery:latest',
                  image: 'artillery-registry:33933/artillery-pro:latest',
                  volumeMounts: [
                    { name: 'artillery-scenario', mountPath: '/data' },
                  ],
                  args: [
                    '-s redis://redis-service:6379',
                    '/data/scenario.json',
                  ],
                },
              ],
              volumes: [
                {
                  name: 'artillery-scenario',
                  configMap: { name: 'artillery-scenario' },
                },
              ],
              restartPolicy: 'Never',
            },
          },
        },
      });
    } catch (err) {
      console.log('Error creating kubernetes job', err);
    }
  }

  listen() {
    this.redis.subscribe('worker-report', (err) => {
      if (err) {
        console.log('error subscribing to channel');
      }
    });

    this.redis.on('message', (channel, message) => {
      console.log('[RECV]', channel, JSON.parse(message));
    });
  }
}

KubeRunCommand.description = 'run a test from a kubernetes cluster';

KubeRunCommand.flags = {
  count: flags.string({
    char: 'n',
    description: 'Number of load generators to launch',
  }),
  config: flags.string({
    char: 'c',
    description: 'Read configuration for the test from the specified file',
  }),
  target: flags.string({
    char: 't',
    description:
      'Set target endpoint. Overrides the target already set in the test script',
  }),
};

KubeRunCommand.args = [{ name: 'script', required: true }];

async function createRedisService(kubeConfig, namespace = 'default') {
  try {
    const coreV1Api = kubeConfig.makeApiClient(k8s.CoreV1Api);

    await coreV1Api.createNamespacedPod(namespace, {
      apiVersion: 'v1',
      kind: 'Pod',
      metadata: {
        name: 'artillery-redis-master',
        labels: {
          app: 'redis',
        },
      },
      spec: {
        containers: [
          {
            name: 'master',
            image: 'redis',
            ports: [{ containerPort: 6379 }],
            env: [{ name: 'master', value: 'true' }],
          },
        ],
      },
    });

    //.then(() => {
    await coreV1Api.createNamespacedService(namespace, {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: 'redis-service',
      },
      spec: {
        ports: [{ port: 6379, targetPort: 6379 }],
        selector: {
          app: 'redis',
        },
      },
    });
    //});
  } catch (err) {
    console.log('Error creating redis-service', err);
  }
}

async function createConfigMap(kubeConfig, scenario, namespace = 'default') {
  if (!scenario) {
    throw new Error('createConfigMap: missing scenario');
  }

  try {
    const coreV1Api = kubeConfig.makeApiClient(k8s.CoreV1Api);

    return coreV1Api.createNamespacedConfigMap(namespace, {
      apiVersion: 'v1',
      kind: 'ConfigMap',
      metadata: {
        name: 'artillery-scenario',
      },
      data: {
        'scenario.json': JSON.stringify(scenario),
      },
    });
  } catch (err) {
    console.log('Error creating kubernetes configMap', err);
  }
}

async function readPayload(script) {
  if (!script.config.payload) {
    return script;
  }

  for (const payloadSpec of script.config.payload) {
    const data = fs.readFileSync(payloadSpec.path, 'utf-8');

    const csvOpts = Object.assign(
      {
        skip_empty_lines:
          typeof payloadSpec.skipEmptyLines === 'undefined'
            ? true
            : payloadSpec.skipEmptyLines,
        cast: typeof payloadSpec.cast === 'undefined' ? true : payloadSpec.cast,
        from_line: payloadSpec.skipHeader === true ? 2 : 1,
        delimiter: payloadSpec.delimiter || ',',
      },
      payloadSpec.options
    );

    // try {
    //   const parsedData = await p(csv)(data, csvOpts);
    //   payloadSpec.data = parsedData;
    // } catch (err) {
    //   throw err;
    // }
  }

  return script;
}

async function waitAndConnectToRedis(redisUrl) {
  await new Promise((resolve) => {
    setTimeout(resolve, 1000 * 10);
  });

  return new Redis(redisUrl);
}

async function createLaunchers(script, payload, opts) {}

async function prepareTestExecutionPlan(inputFiles, flags) {
  let script1 = {};

  for (const fn of inputFiles) {
    const data = await readScript(fn);
    const parsedData = await parseScript(data);
    script1 = await checkConfig(_.merge(script1, parsedData), fn, flags);
  }

  script1 = await addOverrides(script1, flags);

  return script1;
}

async function gracefulShutdown() {
  debug('shutting down 🦑');
  //if (shuttingDown) {
  //  return;
  //}

  debug('Graceful shutdown initiated');

  //shuttingDown = true;
  //telemetry.shutdown()

  // await runner.shutdown();
  await (async function() {
    // for(const r of reporters) {
    //   if (r.cleanup) {
    //     try {
    //       await p(r.cleanup.bind(r))();
    //     } catch (cleanupErr) {
    //       debug(cleanupErr);
    //     }
    //   }
    // }
    debug('Cleanup finished');
    process.exit(global.artillery.suggestedExitCode);
  })();
}

module.exports = KubeRunCommand;
