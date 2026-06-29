/**
 * @fileoverview Main entrypoint to execute probes for different cloud APIs.
 */

const firestore_grpc = require('../google/firestore/v1beta1/firestore_grpc_pb.js');
const spanner_grpc = require('../google/spanner/v1/spanner_grpc_pb.js');
const {GoogleAuth} = require('google-auth-library');
const grpc = require('grpc');
const argparse = require('argparse');
const firestore_probes = require('./firestore_probes');
const spanner_probes = require('./spanner_probes');
const stackdriver_util = require('./stackdriver_util');

const _OAUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform';
const _FIRESTORE_TARGET = 'firestore.googleapis.com:443';
const _SPANNER_TARGET = 'spanner.googleapis.com:443';

/**
 * Retrieves arguments before executing probes.
 * @return {Object} An object containing all the args parsed in.
 */
function getArgs() {
  const parser = new argparse.ArgumentParser({
    version: '0.0.1',
    addHelp: true,
    description: 'Argument parser for grpc gcp prober.',
  });
  parser.addArgument('--api', {help: 'foo bar'});
  return parser.parseArgs();
}

/**
 * Execute a probe function given certain Cloud api.
 * @param {string} api The name of the api provider, e.g. "spanner", "firestore".
 */
function executeProbes(api) {
  const authFactory = new GoogleAuth();
  const util = new stackdriver_util.StackdriverUtil(api);

  authFactory.getApplicationDefault((err, auth) => {
    if (err) {
      console.log('Authentication failed because of ', err);
      return;
    }
    if (auth.createScopedRequired && auth.createScopedRequired()) {
      const scopes = [_OAUTH_SCOPE];
      auth = auth.createScoped(scopes);
    }
    const sslCreds = grpc.credentials.createSsl();
    const callCreds = grpc.credentials.createFromGoogleCredential(auth);
    const channelCreds = grpc.credentials.combineChannelCredentials(
      sslCreds,
      callCreds
    );
    if (api === 'firestore') {
      var client = new firestore_grpc.FirestoreClient(
        _FIRESTORE_TARGET,
        channelCreds
      );
      var probeFunctions = firestore_probes.probeFunctions;
    } else if (api === 'spanner') {
      var client = new spanner_grpc.SpannerClient(
        _SPANNER_TARGET,
        channelCreds
      );
      var probeFunctions = spanner_probes.probeFunctions;
    } else {
      throw new Error('gRPC prober is not implemented for ' + api + ' !');
    }

    const metrics = {};
    const probeNames = Object.keys(probeFunctions);
    const promises = probeNames.map(probeName => {
      probe_function = probeFunctions[probeName];
      return probe_function(client, metrics);
    });

    Promise.all(promises)
      .then(() => {
        util.setSuccess(true);
      })
      .catch(err => {
        util.setSuccess(false);
        util.reportError(err);
      })
      .then(() => {
        util.addMetrics(metrics);
        util.outputMetrics();
      });

    // TODO: if fail, exit probe.
  });
}

const args = getArgs();
executeProbes(args.api);
