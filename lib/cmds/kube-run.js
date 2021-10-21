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

class KubeRunCommand extends Command {
  async run() {
    const { flags, argv } = this.parse(KubeRunCommand);

    const inputFiles = argv.concat(flags.input || [], flags.config || []);

    try {
      const script = await prepareTestExecutionPlan(inputFiles, flags);

      await createK8SJob(script, parseInt(flags.count, 10));

      console.log(`running ${flags.count} workers`)
      //await createK8SJob(script);

      await gracefulShutdown();
    } catch (err) {
      throw err;
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

async function createK8SJob(scenario, count = 1) {
  const kubeConfig = new k8s.KubeConfig();

  //kubeConfig.loadFromCluster();
  kubeConfig.loadFromDefault();

  const batchV1Api = kubeConfig.makeApiClient(k8s.BatchV1Api);
  const coreV1Api = kubeConfig.makeApiClient(k8s.CoreV1Api);

  try {
    await coreV1Api.createNamespacedConfigMap('default', {
      apiVersion: 'v1',
      kind: 'ConfigMap',
      metadata: {
        name: 'artillery-scenario',
      },
      data: {
        'scenario.json': JSON.stringify(scenario),
      },
    });

    await batchV1Api.createNamespacedJob('default', {
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
                image: 'artilleryio/artillery:latest',
                volumeMounts: [
                  { name: 'artillery-scenario', mountPath: '/data' },
                ],
                command: ['artillery', 'run', '/data/scenario.json'],
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
    console.log('Error creating job', err);
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

async function createLaunchers(script, payload, opts) {}

module.exports = KubeRunCommand;
