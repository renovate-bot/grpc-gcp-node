/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

'use strict';

const assert = require('assert');
const getGrpcGcpObjects = require('../../build/src');

/**
 * This is used for testing functions with multiple asynchronous calls that
 * can happen in different orders. This should be passed the number of async
 * function invocations that can occur last, and each of those should call this
 * function's return value
 * @param {function()} done The function that should be called when a test is
 *     complete.
 * @param {number} count The number of calls to the resulting function if the
 *     test passes.
 * @return {function()} The function that should be called at the end of each
 *     sequence of asynchronous functions.
 */
function multiDone(done, count) {
  return function () {
    count -= 1;
    if (count <= 0) {
      done();
    }
  };
}

for (const grpcLibName of ['grpc', '@grpc/grpc-js']) {
  describe('Using ' + grpcLibName, () => {
    const grpc = require(grpcLibName);
    const grpcGcp = getGrpcGcpObjects(grpc);

    const insecureCreds = grpc.credentials.createInsecure();

    describe('grpc-gcp channel factory tests', () => {
      describe('constructor', () => {
        it('should require a string for the first argument', () => {
          assert.doesNotThrow(() => {
            new grpcGcp.GcpChannelFactory('hostname', insecureCreds);
          });
          assert.throws(() => {
            new grpcGcp.GcpChannelFactory();
          }, TypeError);
          assert.throws(() => {
            new grpcGcp.GcpChannelFactory(5);
          });
        });
        it('should require a credential for the second argument', () => {
          assert.doesNotThrow(() => {
            new grpcGcp.GcpChannelFactory('hostname', insecureCreds);
          });
          assert.throws(() => {
            new grpcGcp.GcpChannelFactory('hostname', 5);
          });
          assert.throws(() => {
            new grpcGcp.GcpChannelFactory('hostname');
          });
        });
        it('should accept an object for the third argument', () => {
          assert.doesNotThrow(() => {
            new grpcGcp.GcpChannelFactory('hostname', insecureCreds, {});
          });
          assert.throws(() => {
            new grpcGcp.GcpChannelFactory('hostname', insecureCreds, 'abc');
          });
        });
        it('should only accept objects with string or int values', () => {
          assert.doesNotThrow(() => {
            new grpcGcp.GcpChannelFactory('hostname', insecureCreds, {
              key: 'value',
            });
          });
          assert.doesNotThrow(() => {
            new grpcGcp.GcpChannelFactory('hostname', insecureCreds, {key: 5});
          });
        });
        it('should succeed without the new keyword', () => {
          assert.doesNotThrow(() => {
            const channel = new grpcGcp.GcpChannelFactory(
              'hostname',
              insecureCreds
            );
            assert(channel instanceof grpcGcp.GcpChannelFactory);
          });
        });

        it('should build min channels', () => {
          const channel = new grpcGcp.GcpChannelFactory(
            'hostname',
            insecureCreds,
            {
              gcpApiConfig: grpcGcp.createGcpApiConfig({
                channelPool: {
                  minSize: 3,
                },
              }),
            }
          );
          assert.equal(channel.channelRefs.length, 3);
        });
      });
      describe('affinity bindings', () => {
        let channelFactory;
        let mockChannelRef;

        beforeEach(() => {
          channelFactory = new grpcGcp.GcpChannelFactory(
            'hostname',
            insecureCreds,
            {}
          );
          mockChannelRef = channelFactory.getChannelRef();
        });

        afterEach(() => {
          channelFactory.close();
        });

        describe('bindIfUnbound', () => {
          it('should return true and increment affinityCount for a new key', () => {
            const initialCount = mockChannelRef.getAffinityCount();
            const result = channelFactory.bindIfUnbound(
              mockChannelRef,
              'txn-1'
            );
            assert.strictEqual(result, true);
            assert.strictEqual(
              mockChannelRef.getAffinityCount(),
              initialCount + 1
            );
            assert.strictEqual(channelFactory.isBound('txn-1'), true);
          });

          it('should return false and not increment affinityCount if key is already bound', () => {
            channelFactory.bindIfUnbound(mockChannelRef, 'txn-2');
            const countAfterFirstBind = mockChannelRef.getAffinityCount();

            // Try to bind the same key again
            const result = channelFactory.bindIfUnbound(
              mockChannelRef,
              'txn-2'
            );
            assert.strictEqual(result, false);
            assert.strictEqual(
              mockChannelRef.getAffinityCount(),
              countAfterFirstBind
            );
          });
        });

        describe('unbind', () => {
          it('should immediately delete key if affinityCount hits zero', () => {
            channelFactory.bindIfUnbound(mockChannelRef, 'txn-3');
            assert.strictEqual(channelFactory.isBound('txn-3'), true);

            channelFactory.unbind('txn-3');
            assert.strictEqual(channelFactory.isBound('txn-3'), false);
          });

          it('should wait for affinityCount to hit zero before deleting key for legacy bind', () => {
            // Legacy bind increments blindly
            channelFactory.bind(mockChannelRef, 'txn-4');
            channelFactory.bind(mockChannelRef, 'txn-4');

            assert.strictEqual(channelFactory.isBound('txn-4'), true);

            // First unbind
            channelFactory.unbind('txn-4');
            assert.strictEqual(channelFactory.isBound('txn-4'), true); // Still bound!

            // Second unbind
            channelFactory.unbind('txn-4');
            assert.strictEqual(channelFactory.isBound('txn-4'), false); // Now deleted
          });
        });

        describe('idle key cleanup', () => {
          let originalDateNow;
          beforeEach(() => {
            originalDateNow = Date.now;
          });
          afterEach(() => {
            Date.now = originalDateNow;
          });

          it('should cleanup idle keys after 3 minutes', () => {
            const factory = new grpcGcp.GcpChannelFactory(
              'hostname',
              insecureCreds,
              {}
            );
            Date.now = () => 1000; // Start at 1000ms
            const ref = factory.getChannelRef();
            factory.bindIfUnbound(ref, 'idle-key-1');
            assert.strictEqual(factory.isBound('idle-key-1'), true);

            // Advance time by slightly more than 3 minutes (180000ms)
            Date.now = () => 1000 + 180001;

            // Manually trigger the private cleanup method
            factory.cleanupIdleKeys();

            assert.strictEqual(factory.isBound('idle-key-1'), false);
            factory.close();
          });

          it('should not cleanup keys that are accessed within 3 minutes', () => {
            const factory = new grpcGcp.GcpChannelFactory(
              'hostname',
              insecureCreds,
              {}
            );
            Date.now = () => 1000;
            const ref = factory.getChannelRef();
            factory.bindIfUnbound(ref, 'active-key-1');
            assert.strictEqual(factory.isBound('active-key-1'), true);

            // Advance time by 2 minutes and access it
            Date.now = () => 1000 + 120000;
            factory.getChannelRef('active-key-1'); // Updates lastAccessed

            // Advance time by another 2 minutes (total 4 since bind, but 2 since last access)
            Date.now = () => 1000 + 240000;
            factory.cleanupIdleKeys();

            // It should still be bound
            assert.strictEqual(factory.isBound('active-key-1'), true);
            factory.close();
          });

          it('should NOT cleanup regular session keys bound via bind', () => {
            const factory = new grpcGcp.GcpChannelFactory(
              'hostname',
              insecureCreds,
              {}
            );
            Date.now = () => 1000;
            const ref = factory.getChannelRef();
            factory.bind(ref, 'regular-session-key');
            assert.strictEqual(factory.isBound('regular-session-key'), true);

            // Advance time by slightly more than 3 minutes (180000ms)
            Date.now = () => 1000 + 180001;

            // Manually trigger the private cleanup method
            factory.cleanupIdleKeys();

            // It should still be bound because bind() ignores TTL tracking
            assert.strictEqual(factory.isBound('regular-session-key'), true);
            factory.close();
          });

          it('should correctly isolate cleanup when both custom and regular keys are present', () => {
            const factory = new grpcGcp.GcpChannelFactory(
              'hostname',
              insecureCreds,
              {}
            );
            Date.now = () => 1000;
            const ref = factory.getChannelRef();

            // Bind a custom transaction key
            factory.bindIfUnbound(ref, 'custom-tx-key');
            // Bind a regular session key
            factory.bind(ref, 'regular-session-key');

            assert.strictEqual(factory.isBound('custom-tx-key'), true);
            assert.strictEqual(factory.isBound('regular-session-key'), true);

            // Advance time by slightly more than 3 minutes (180000ms)
            Date.now = () => 1000 + 180001;

            // Mock getAffinityCount to 0 to bypass the legacy grpc-gcp-node
            // bug where unbind doesn't delete keys if affinityCount > 0
            ref.getAffinityCount = () => 0;
            factory.cleanupIdleKeys();

            // Custom key should be cleaned up, regular key should survive
            assert.strictEqual(factory.isBound('custom-tx-key'), false);
            assert.strictEqual(factory.isBound('regular-session-key'), true);
            factory.close();
          });

          it('getChannelRef should not accidentally start a timer for regular session keys', () => {
            const factory = new grpcGcp.GcpChannelFactory(
              'hostname',
              insecureCreds,
              {}
            );
            Date.now = () => 1000;
            const ref = factory.getChannelRef();

            factory.bind(ref, 'regular-session-key');

            // Advance time by 2 minutes and access it
            Date.now = () => 1000 + 120000;
            factory.getChannelRef('regular-session-key'); // Simulating network activity

            // Advance time by another 2 minutes (total 4)
            Date.now = () => 1000 + 240000;

            factory.cleanupIdleKeys();

            // It should still be bound because getChannelRef doesn't track keys missing from TTL map
            assert.strictEqual(factory.isBound('regular-session-key'), true);
            factory.close();
          });
        });
      });
      describe('close', () => {
        let channel;
        beforeEach(() => {
          channel = new grpcGcp.GcpChannelFactory(
            'hostname',
            insecureCreds,
            {}
          );
        });
        it('should succeed silently', () => {
          assert.doesNotThrow(() => {
            channel.close();
          });
        });
        it('should be idempotent', () => {
          assert.doesNotThrow(() => {
            channel.close();
            channel.close();
          });
        });
      });
      describe('getTarget', () => {
        let channel;
        beforeEach(() => {
          channel = new grpcGcp.GcpChannelFactory(
            'hostname',
            insecureCreds,
            {}
          );
        });
        it('should return a string', () => {
          assert.strictEqual(typeof channel.getTarget(), 'string');
        });
      });
      describe('getConnectivityState', () => {
        let channel;
        beforeEach(() => {
          channel = new grpcGcp.GcpChannelFactory(
            'hostname',
            insecureCreds,
            {}
          );
        });
        it('should return IDLE for a new channel', () => {
          assert.strictEqual(
            channel.getConnectivityState(),
            grpc.connectivityState.IDLE
          );
        });
      });
      describe('watchConnectivityState', () => {
        let channel;
        beforeEach(() => {
          channel = new grpcGcp.GcpChannelFactory(
            'localhost',
            insecureCreds,
            {}
          );
        });
        afterEach(() => {
          channel.close();
        });
        it('should throw an error if no channels are available', done => {
          channel.channelRefs = [];
          channel.watchConnectivityState(0, new Date(), err => {
            assert(err instanceof Error);
            assert.strictEqual(
              err.message,
              'Cannot watch connectivity state because there are no channels.'
            );
            done();
          });
        });
        it('should resolve immediately if the state is different', done => {
          const fakeState = grpc.connectivityState.READY;
          channel.getConnectivityState = function () {
            return grpc.connectivityState.IDLE;
          };
          channel.watchConnectivityState(fakeState, 1000, err => {
            assert.ifError(err);
            done();
          });
        });
        it('should call channel.watchConnectivityState', done => {
          const fakeState = grpc.connectivityState.READY;
          channel.getConnectivityState = function () {
            return fakeState;
          };
          channel.channelRefs.forEach(channelRef => {
            channelRef.channel.getConnectivityState = function (connect) {
              assert.strictEqual(connect, false);
              return fakeState;
            };
            channelRef.channel.watchConnectivityState = function (s, d, cb) {
              assert.strictEqual(s, fakeState);
              assert.strictEqual(d, 1000);
              channel.getConnectivityState = function () {
                return grpc.connectivityState.IDLE;
              };
              setImmediate(cb);
            };
          });
          channel.watchConnectivityState(fakeState, 1000, done);
        });
      });
      describe('createCall', () => {
        let channel;
        beforeEach(() => {
          channel = new grpcGcp.GcpChannelFactory(
            'localhost',
            insecureCreds,
            {}
          );
        });
        afterEach(() => {
          channel.close();
        });
        it('should return grpc.Call', () => {
          assert.throws(() => {
            channel.createCall();
          }, TypeError);
          assert.throws(() => {
            channel.createCall('method');
          }, TypeError);
          assert.doesNotThrow(() => {
            channel.createCall('method', new Date());
          });
          assert.doesNotThrow(() => {
            channel.createCall('method', 0);
          });
          assert.doesNotThrow(() => {
            channel.createCall('method', new Date(), 'host_override');
          });
        });
      });
    });
  });
}
