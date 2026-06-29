/**
 * @fileoverview Spanner probes for Node.js that cloudprober will execute.
 *
 * Each method implements grpc client calls to Spanner API. The latency for
 * each client call will be output to stackdriver as metrics.
 */

const spanner = require('../google/spanner/v1/spanner_pb.js');
const _DATABASE =
  'projects/grpc-prober-testing/instances/weiranf-instance/databases/test-db';
const _TEST_USERNAME = 'test_username';
const Promise = require('bluebird');

/**
 * Probes to test session related grpc calls from Spanner client.
 * @param {spanner_grpc_pb.SpannerClient} client An instance of SpannerClient.
 * @param {Object} metrics A map of metrics.
 * @return {Promise} A promise after a sequence of client calls.
 */
function sessionManagement(client, metrics) {
  let sessionName;

  const createSession = () => {
    return new Promise((resolve, reject) => {
      const createSessionRequest = new spanner.CreateSessionRequest();
      createSessionRequest.setDatabase(_DATABASE);
      const start = new Date();
      client.createSession(createSessionRequest, (error, session) => {
        if (error) {
          reject(error);
        } else {
          const latency = new Date() - start;
          metrics['create_session_latency_ms'] = latency;
          sessionName = session.getName();
          resolve();
        }
      });
    });
  };

  const getSession = () => {
    return new Promise((resolve, reject) => {
      const getSessionRequest = new spanner.GetSessionRequest();
      getSessionRequest.setName(sessionName);
      start = new Date();
      client.getSession(getSessionRequest, (error, sessionResult) => {
        if (error) {
          reject(error);
        } else {
          const latency = new Date() - start;
          metrics['get_session_latency_ms'] = latency;
          if (sessionResult.getName() != sessionName) {
            reject(
              new Error(
                'client.getSession has incorrect result: ' +
                  sessionResult.getName()
              )
            );
          }
          resolve();
        }
      });
    });
  };

  const deleteSession = () => {
    return new Promise((resolve, reject) => {
      const deleteSessionRequest = new spanner.DeleteSessionRequest();
      deleteSessionRequest.setName(sessionName);
      start = new Date();
      client.deleteSession(deleteSessionRequest, error => {
        if (error) {
          reject(error);
        } else {
          const latency = new Date() - start;
          metrics['delete_session_latency_ms'] = latency;
          resolve();
        }
      });
    });
  };

  return createSession()
    .then(() => {
      return getSession();
    })
    .finally(() => {
      if (sessionName) {
        return deleteSession();
      }
    });
}

exports.probeFunctions = {
  sessionManagement: sessionManagement,
};
