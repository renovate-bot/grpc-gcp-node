/**
 * @fileoverview Firestore probes for Node.js that cloudprober will execute.
 *
 * Each method implements grpc client calls to Firestore API. The latency for
 * each client call will be output to stackdriver as metrics.
 */

const firestore = require('../google/firestore/v1beta1/firestore_pb.js');
const _PARENT_RESOURCE =
  'projects/grpc-prober-testing/databases/(default)/documents';

/**
 * Probes to test session related grpc calls from Spanner client.
 * @param {spanner_grpc_pb.SpannerClient} client An instance of FirestoreClient.
 * @param {Object} metrics A map of metrics.
 * @return {Promise} A promise after a sequence of client calls.
 */
function documents(client, metrics) {
  return new Promise((resolve, reject) => {
    const listDocsRequest = new firestore.ListDocumentsRequest();
    listDocsRequest.setParent(_PARENT_RESOURCE);
    const start = new Date();
    client.listDocuments(listDocsRequest, (error, response) => {
      if (error) {
        reject(error);
      } else {
        const latency = new Date() - start;
        metrics['list_documents_latency_ms'] = latency;
        const docArray = response.getDocumentsList();
        if (!docArray || !docArray.length) {
          reject(
            new Error('ListDocumentsResponse should have more than 1 document')
          );
        }
        resolve();
      }
    });
  });
}

// exports.documents = documents;
exports.probeFunctions = {
  documents: documents,
};
