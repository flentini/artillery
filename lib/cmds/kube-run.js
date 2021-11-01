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
const createConsoleReporter = require('../../lib/console-reporter');
const debug = require('debug')('commands:kube-run');
const _ = require('lodash');
const net = require('net');
const Redis = require('ioredis');
const EventEmitter = require('events');

const k8sObjectsNames = {
  namespace: 'default',
  redisPod: 'artillery-redis-master',
  redisService: 'artillery-redis',
  configMap: 'artillery-scenario',
  workerJob: 'artillery-worker',
};

class KubeRunCommand extends Command {
  async run() {
    process.once('SIGINT', this.gracefulShutdown.bind(this));
    process.once('SIGTERM', this.gracefulShutdown.bind(this));

    const { flags, argv } = this.parse(KubeRunCommand);
    const inputFiles = argv.concat(flags.input || [], flags.config || []);
    const script = await prepareTestExecutionPlan(inputFiles, flags);

    this.ee = new EventEmitter();

    this.kubeConfig = new k8s.KubeConfig();
    //kubeConfig.loadFromCluster();
    this.kubeConfig.loadFromDefault();

    this.k8sClients = {
      coreV1Api: this.kubeConfig.makeApiClient(k8s.CoreV1Api),
      batchV1Api: this.kubeConfig.makeApiClient(k8s.BatchV1Api),
    };

    this.metricsByPeriod = {}; // individual intermediates by worker
    this.mergedPeriodMetrics = []; // merged intermediates for a period

    // TODO createConsoleReporter needs opts?
    this.consoleReporter = createConsoleReporter(this.ee, {});

    try {

      this.jobsCount = parseInt(flags.count, 10) || 1;
      this.jobsDone = 0;

      debug('creating redis dependencies...');
      await this.createRedisService();

      debug('creating configmap...');
      await this.createConfigMap(script);

      // TODO check for something better than port forwarding
      debug('creating port forwarding...');
      const portForwardDetails = await this.createPortForward({
        podName: k8sObjectsNames.redisPod,
        podPort: 6379,
      });

      debug('connecting to redis...');
      // TODO wait until the redis pod is up
      this.redis = await waitAndConnectToRedis(
        `redis://0.0.0.0:${portForwardDetails.port}`
      );

      this.listen();

      debug('creating k8s job...');
      await this.createK8SJob(this.jobsCount);
    } catch (err) {
      console.log('kube-run error:', err);
      console.log('deleting k8s resources...');
      await this.gracefulShutdown();

      throw err;
    }
  }

  allWorkersDone() {
    return this.jobsDone === this.jobsCount;
  }

  async deleteK8SResources(namespace = k8sObjectsNames.namespace) {
    const args = [
      namespace,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      {
        apiVersion: 'v1',
        //propagationPolicy: 'Background',
        propagationPolicy: 'Foreground',
      },
    ];

    debug('deleting k8s resources')
    await Promise.all([
      ignorek8s404(
        this.k8sClients.coreV1Api.deleteNamespacedPod(
          k8sObjectsNames.redisPod,
          ...args
        )
      ),
      ignorek8s404(
        this.k8sClients.coreV1Api.deleteNamespacedService(
          k8sObjectsNames.redisService,
          ...args
        )
      ),
      ignorek8s404(
        this.k8sClients.coreV1Api.deleteNamespacedConfigMap(
          k8sObjectsNames.configMap,
          ...args
        )
      ),
      ignorek8s404(
        this.k8sClients.batchV1Api.deleteNamespacedJob(
          k8sObjectsNames.workerJob,
          ...args
        )
      ),
    ]);
  }

  async createPortForward(options = {}, namespace = k8sObjectsNames.namespace) {
    const p = new k8s.PortForward(this.kubeConfig);

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

  async createK8SJob(count = 1, namespace = k8sObjectsNames.namespace) {
    try {
      return this.k8sClients.batchV1Api.createNamespacedJob(namespace, {
        apiVersion: 'batch/v1',
        kind: 'Job',
        metadata: {
          name: k8sObjectsNames.workerJob,
          labels: {
            app: k8sObjectsNames.workerJob,
          },
        },
        spec: {
          completions: count,
          parallelism: count,
          template: {
            spec: {
              containers: [
                {
                  name: k8sObjectsNames.workerJob,
                  // image: 'artilleryio/artillery:latest',
                  // image: 'artillery-registry:5000/artillery-pro:latest',
                  image: '734505410937.dkr.ecr.eu-west-1.amazonaws.com/artillery-pro-test:latest',
                  volumeMounts: [
                    { name: k8sObjectsNames.configMap, mountPath: '/data' },
                  ],
                  args: [
                    `-sredis://${k8sObjectsNames.redisService}:6379`,
                    '/data/scenario.json',
                  ],
                },
              ],
              volumes: [
                {
                  name: k8sObjectsNames.configMap,
                  configMap: { name: k8sObjectsNames.configMap },
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

  async createRedisService(namespace = k8sObjectsNames.namespace) {
    try {
      await this.k8sClients.coreV1Api.createNamespacedPod(namespace, {
        apiVersion: 'v1',
        kind: 'Pod',
        metadata: {
          name: k8sObjectsNames.redisPod,
          labels: {
            app: k8sObjectsNames.redisPod,
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

      await this.k8sClients.coreV1Api.createNamespacedService(namespace, {
        apiVersion: 'v1',
        kind: 'Service',
        metadata: {
          name: k8sObjectsNames.redisService,
        },
        spec: {
          ports: [{ port: 6379, targetPort: 6379 }],
          selector: {
            app: k8sObjectsNames.redisPod,
          },
        },
      });
    } catch (err) {
      console.log('Error creating redis-service', err);
    }
  }

  async createConfigMap(scenario, namespace = k8sObjectsNames.namespace) {
    if (!scenario) {
      throw new Error('createConfigMap: missing scenario');
    }

    try {
      return this.k8sClients.coreV1Api.createNamespacedConfigMap(namespace, {
        apiVersion: 'v1',
        kind: 'ConfigMap',
        metadata: {
          name: k8sObjectsNames.configMap,
        },
        data: {
          'scenario.json': JSON.stringify(scenario),
        },
      });
    } catch (err) {
      console.log('Error creating kubernetes configMap', err);
    }
  }

  listen() {
    this.redis.subscribe('worker-report', (err) => {
      if (err) {
        console.log('error subscribing to channel', err);
      }
    });

    this.redis.on('message', async (_, message) => {
      try {
        const parsedMessage = JSON.parse(message);
        this.handleMessage(parsedMessage);

        if (this.allWorkersDone()) {
          await this.gracefulShutdown();
        }
      } catch (err) {
        console.log('Error parsing worker-report message', err);
      }
    });
  }

  handleMessage(message) {
    const { body } = message;

    if (body.event === 'workerStats') {
      debug('processing workerStats event');
      // v2 SSMS stats
      const workerStats = global.artillery.__SSMS.deserializeMetrics(
        body.stats
      );
      const period = workerStats.period;

      debug(workerStats);

      if (typeof this.metricsByPeriod[period] === 'undefined') {
        this.metricsByPeriod[period] = [];
      }

      this.metricsByPeriod[period].push(workerStats);

      debug('metricsByPeriod:');
      debug(this.metricsByPeriod);
      debug('number of periods processed');
      debug(Object.keys(this.metricsByPeriod));
      debug('number of metrics collections:');
      debug(this.metricsByPeriod[period].length, this.jobsCount);

      if (this.metricsByPeriod[period].length >= this.jobsCount) {
        debug('have metrics from all workers for this period', period);

        const stats = global.artillery.__SSMS.mergeBuckets(
          this.metricsByPeriod[String(period)]
        )[String(period)];

        this.mergedPeriodMetrics.push(stats);
        // summarize histograms for console reporter
        stats.summaries = {};

        for (const [name, value] of Object.entries(stats.histograms || {})) {
          const summary = global.artillery.__SSMS.summarizeHistogram(value);
          stats.summaries[name] = summary;
          delete this.metricsByPeriod[String(period)];
        }

        debug('Emitting stats event');
        this.ee.emit('stats', stats);
      } else {
        debug('Waiting for more workerStats before emitting stats event');
      }
    }

    if (body.event === 'workerDone' || body.event === 'workerError') {
      this.jobsDone++;

      debug('ready to emit done event');
      debug('mergedPeriodMetrics');
      debug(this.mergedPeriodMetrics);

      // check if all the workers are done
      // Merge by period, then compress and emit
      const stats = global.artillery.__SSMS.pack(this.mergedPeriodMetrics);
      stats.summaries = {};
      for (const [name, value] of Object.entries(stats.histograms || {})) {
        const summary = global.artillery.__SSMS.summarizeHistogram(value);
        stats.summaries[name] = summary;
      }

      if (this.allWorkersDone()) {
        // print the aggregate report
        this.ee.emit('done', stats);
      }
    }
  }

  async gracefulShutdown() {
    debug('shutting down 🦑');

    if (this.shuttingDown) {
      return;
    }

    debug('Graceful shutdown initiated');

    this.shuttingDown = true;

    console.log(this.deleteK8SResources)
    await this.deleteK8SResources();

    this.forwardServer.close();
    //telemetry.shutdown()

    await (async function() {
      debug('Cleanup finished');
      process.exit(global.artillery.suggestedExitCode);
    })();
  }
}

KubeRunCommand.description = 'run a test from a kubernetes cluster';

KubeRunCommand.flags = {
  // TODO add k8s namespace flag

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

async function waitAndConnectToRedis(redisUrl) {
  // TODO instead of waiting a fixed amount of time, poll k8s to check when the service is actually up
  await new Promise((resolve) => {
    setTimeout(resolve, 1000 * 10);
  });

  return new Redis(redisUrl);
}

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

// try to be idempotent and to start with a clean slate, deleting all the required resources at start if they exist.
// if they don't exist k8s will throw a 404 that can be ignored
async function ignorek8s404(fn) {
  try {
    await fn;
  } catch (err) {
    debug('Kubernetes error:', err);

    if (err.statusCode === 404) {
      return;
    }

    throw err;
  }
}

module.exports = KubeRunCommand;
