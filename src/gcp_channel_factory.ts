/**
 * @license
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

import * as grpcType from '@grpc/grpc-js';
import {promisify} from 'util';

import {ChannelRef} from './channel_ref';
import * as protoRoot from './generated/grpc_gcp';
import {connectivityState} from '@grpc/grpc-js';
import ApiConfig = protoRoot.grpc.gcp.ApiConfig;
import IAffinityConfig = protoRoot.grpc.gcp.IAffinityConfig;

const CLIENT_CHANNEL_ID = 'grpc_gcp.client_channel.id';
const CLEANUP_INTERVAL_MS = 30000; // Sweep every 30 seconds
const IDLE_TIMEOUT_MS = 180000; // Keys are considered idle if unused for more than 3 minutes

export type GrpcModule = typeof grpcType;

export interface GcpChannelFactoryInterface extends grpcType.ChannelInterface {
  getChannelRef(affinityKey?: string): ChannelRef;
  getAffinityConfig(methodName: string): IAffinityConfig;
  bind(channelRef: ChannelRef, affinityKey: string): void;
  unbind(boundKey?: string): void;
  shouldRequestDebugHeaders(lastRequested: Date | null): boolean;
  isBound(affinityKey: string): boolean;
  bindIfUnbound(channelRef: ChannelRef, affinityKey: string): boolean;
}

export interface GcpChannelFactoryConstructor {
  new (
    address: string,
    credentials: grpcType.ChannelCredentials,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options: any
  ): GcpChannelFactoryInterface;
}

export function getGcpChannelFactoryClass(
  grpc: GrpcModule
): GcpChannelFactoryConstructor {
  /**
   * A channel management factory that implements grpc.Channel APIs.
   */
  return class GcpChannelFactory implements GcpChannelFactoryInterface {
    private minSize: number;
    private maxSize: number;
    private maxConcurrentStreamsLowWatermark: number;
    private options: {};
    private methodToAffinity: {[key: string]: IAffinityConfig} = {};
    private affinityKeyToChannelRef = new Map<string, ChannelRef>();
    private affinityKeyLastAccessed = new Map<string, number>();
    private cleanupTimer: ReturnType<typeof setInterval>;
    private channelRefs: ChannelRef[] = [];
    private target: string;
    private credentials: grpcType.ChannelCredentials;
    private debugHeaderIntervalSecs: number;

    /**
     * @param address The address of the server to connect to.
     * @param credentials Channel credentials to use when connecting
     * @param options A map of channel options.
     */
    constructor(
      address: string,
      credentials: grpcType.ChannelCredentials,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      options: any
    ) {
      if (!options) {
        options = {};
      }
      if (typeof options !== 'object') {
        throw new TypeError(
          'Channel options must be an object with string keys and integer or string values'
        );
      }
      this.minSize = 1;
      this.maxSize = 10;
      this.maxConcurrentStreamsLowWatermark = 100;
      this.debugHeaderIntervalSecs = 0;
      const gcpApiConfig = options.gcpApiConfig;
      if (gcpApiConfig) {
        if (gcpApiConfig.channelPool) {
          const channelPool = gcpApiConfig.channelPool;
          if (channelPool.minSize) this.minSize = channelPool.minSize;
          if (channelPool.maxSize) this.maxSize = channelPool.maxSize;
          if (channelPool.maxConcurrentStreamsLowWatermark) {
            this.maxConcurrentStreamsLowWatermark =
              channelPool.maxConcurrentStreamsLowWatermark;
          }

          if (this.maxSize < this.minSize) {
            throw new Error(
              'Invalid channelPool config: minSize must <= maxSize'
            );
          }
          this.debugHeaderIntervalSecs =
            channelPool.debugHeaderIntervalSecs || 0;
        }
        this.initMethodToAffinityMap(gcpApiConfig);
      }

      delete options.gcpApiConfig;
      this.options = options;
      this.target = address;
      this.credentials = credentials;

      // Create initial channels
      for (let i = 0; i < this.minSize; i++) {
        this.addChannel();
      }

      // Periodically clean up affinity keys that haven't been accessed for a while to prevent unbounded growth.
      this.cleanupTimer = setInterval(
        () => this.cleanupIdleKeys(),
        CLEANUP_INTERVAL_MS
      );
      if (this.cleanupTimer.unref) {
        this.cleanupTimer.unref();
      }
    }

    getChannelzRef() {
      return this.channelRefs[0].getChannel().getChannelzRef();
    }

    private initMethodToAffinityMap(gcpApiConfig: ApiConfig): void {
      const methodList = gcpApiConfig.method;
      if (methodList) {
        for (let i = 0; i < methodList.length; i++) {
          const method = methodList[i];
          const nameList = method.name;
          if (nameList) {
            for (let j = 0; j < nameList.length; j++) {
              const methodName = nameList[j];
              if (method.affinity) {
                this.methodToAffinity[methodName] = method.affinity;
              }
            }
          }
        }
      }
    }

    /**
     * Picks a grpc channel from the pool and wraps it with ChannelRef.
     * @param affinityKey Affinity key to get the bound channel.
     * @return Wrapper containing the grpc channel.
     */
    getChannelRef(affinityKey?: string): ChannelRef {
      if (affinityKey && this.affinityKeyLastAccessed.has(affinityKey)) {
        this.affinityKeyLastAccessed.set(affinityKey, Date.now());
      }
      if (affinityKey && this.affinityKeyToChannelRef.has(affinityKey)) {
        // Chose an bound channel if affinityKey is specified.
        return this.affinityKeyToChannelRef.get(affinityKey)!;
      }

      // Find the channelRef that has the least busy channel.
      let minRef = this.channelRefs[0];
      for (let i = 1; i < this.channelRefs.length; i++) {
        if (
          this.channelRefs[i].getActiveStreamsCount() <
          minRef.getActiveStreamsCount()
        ) {
          minRef = this.channelRefs[i];
        }
      }

      const size = this.channelRefs.length;
      if (
        size > 0 &&
        minRef.getActiveStreamsCount() < this.maxConcurrentStreamsLowWatermark
      ) {
        return minRef;
      }

      // If all existing channels are busy, and channel pool still has capacity,
      // create a new channel in the pool.
      if (size < this.maxSize) {
        return this.addChannel();
      } else {
        return minRef;
      }
    }

    private cleanupIdleKeys() {
      const now = Date.now();
      // Store keys to unbind in an array before iterating to avoid
      // modifying the Map while we are actively iterating over its entries.
      const keysToUnbind: string[] = [];
      for (const [
        key,
        lastAccessed,
      ] of this.affinityKeyLastAccessed.entries()) {
        if (now - lastAccessed > IDLE_TIMEOUT_MS) {
          // if a key is not in used for more than 3 minutes remove its binding
          keysToUnbind.push(key);
        }
      }
      keysToUnbind.forEach(key => this.unbind(key));
    }

    /**
     * Create a new channel and add it to the pool.
     * @private
     */
    private addChannel(): ChannelRef {
      const size = this.channelRefs.length;
      const channelOptions = Object.assign(
        {[CLIENT_CHANNEL_ID]: size},
        this.options
      );
      const grpcChannel = new grpc.Channel(
        this.target,
        this.credentials,
        channelOptions
      );
      const channelRef = new ChannelRef(grpcChannel, size);
      this.channelRefs.push(channelRef);

      if (this.debugHeaderIntervalSecs) {
        this.setupDebugHeadersOnChannelTransition(channelRef);
      }

      return channelRef;
    }

    private setupDebugHeadersOnChannelTransition(channel: ChannelRef) {
      if (channel.isClosed()) {
        return;
      }

      const currentState = channel.getChannel().getConnectivityState(false);
      if (currentState === connectivityState.SHUTDOWN) {
        return;
      }

      channel
        .getChannel()
        .watchConnectivityState(currentState, Infinity, () => {
          channel.forceDebugHeadersOnNextRequest();
          this.setupDebugHeadersOnChannelTransition(channel);
        });
    }

    /**
     * Get AffinityConfig associated with a certain method.
     * @param methodName Method name of the request.
     */
    getAffinityConfig(methodName: string): IAffinityConfig {
      return this.methodToAffinity[methodName];
    }

    shouldRequestDebugHeaders(lastRequested: Date | null): boolean {
      if (this.debugHeaderIntervalSecs < 0) return true;
      else if (this.debugHeaderIntervalSecs === 0) return false;
      else if (!lastRequested) return true;

      return (
        new Date().getTime() - lastRequested.getTime() >
        this.debugHeaderIntervalSecs * 1000
      );
    }

    /**
     * Bind channel with affinity key.
     * @param channelRef ChannelRef instance that contains the grpc channel.
     * @param affinityKey The affinity key used for binding the channel.
     */
    bind(channelRef: ChannelRef, affinityKey: string): void {
      if (!affinityKey || !channelRef) return;
      let existingChannelRef = this.affinityKeyToChannelRef.get(affinityKey);
      if (!existingChannelRef) {
        this.affinityKeyToChannelRef.set(affinityKey, channelRef);
        existingChannelRef = channelRef;
      }
      // We explicitly do NOT add regular session keys to affinityKeyLastAccessed
      // to ensure they are not cleaned up by the 3-minute TTL idle sweep.
      existingChannelRef.affinityCountIncr();
    }

    /**
     * Checks if an affinity key is already bound to a channel.
     */
    isBound(affinityKey: string): boolean {
      return this.affinityKeyToChannelRef.has(affinityKey);
    }

    /**
     * Binds an affinity key to a channel if it is not already bound.
     * This ensures atomic binding on the first request of a transaction,
     * preventing race conditions when multiple concurrent requests start.
     *
     * @param channelRef The channel to bind to.
     * @param affinityKey The unique key for the transaction.
     */
    bindIfUnbound(channelRef: ChannelRef, affinityKey: string): boolean {
      if (!affinityKey || !channelRef) return false;
      if (!this.affinityKeyToChannelRef.has(affinityKey)) {
        this.affinityKeyToChannelRef.set(affinityKey, channelRef);
        this.affinityKeyLastAccessed.set(affinityKey, Date.now());
        channelRef.affinityCountIncr();
        return true;
      }
      return false;
    }

    /**
     * Unbind channel with affinity key.
     * @param boundKey Affinity key bound to a channel.
     */
    unbind(boundKey?: string): void {
      if (!boundKey) return;
      const boundChannelRef = this.affinityKeyToChannelRef.get(boundKey);
      if (boundChannelRef) {
        boundChannelRef.affinityCountDecr();
        // SAFEGUARD: If it's a custom multiplexed transaction key (which exists in affinityKeyLastAccessed),
        // we force delete it to prevent memory leaks, because custom keys are ephemeral.
        // For legacy keys (regular Spanner sessions), we preserve the old behavior
        // which waits for the entire channel's affinityCount to hit 0 before deleting.
        if (
          this.affinityKeyLastAccessed.has(boundKey) ||
          boundChannelRef.getAffinityCount() <= 0
        ) {
          this.affinityKeyToChannelRef.delete(boundKey);
          this.affinityKeyLastAccessed.delete(boundKey);
        }
      }
    }

    /**
     * Close all channels in the channel pool.
     */
    close(): void {
      if (this.cleanupTimer) {
        clearInterval(this.cleanupTimer);
      }
      this.channelRefs.forEach(ref => {
        ref.close();
      });
    }

    getTarget(): string {
      return this.target;
    }

    /**
     * Get the current connectivity state of the channel pool.
     * @param tryToConnect If true, the channel will start connecting if it is
     *     idle. Otherwise, idle channels will only start connecting when a
     *     call starts.
     * @return connectivity state of channel pool.
     */
    getConnectivityState(tryToConnect: boolean): grpcType.connectivityState {
      let ready = 0;
      let idle = 0;
      let connecting = 0;
      let transientFailure = 0;
      let shutdown = 0;

      for (let i = 0; i < this.channelRefs.length; i++) {
        const grpcChannel = this.channelRefs[i].getChannel();
        const state = grpcChannel.getConnectivityState(tryToConnect);
        switch (state) {
          case grpc.connectivityState.READY:
            ready++;
            break;
          case grpc.connectivityState.SHUTDOWN:
            shutdown++;
            break;
          case grpc.connectivityState.TRANSIENT_FAILURE:
            transientFailure++;
            break;
          case grpc.connectivityState.CONNECTING:
            connecting++;
            break;
          case grpc.connectivityState.IDLE:
            idle++;
            break;
          default:
            break;
        }
      }

      if (ready > 0) {
        return grpc.connectivityState.READY;
      } else if (connecting > 0) {
        return grpc.connectivityState.CONNECTING;
      } else if (transientFailure > 0) {
        return grpc.connectivityState.TRANSIENT_FAILURE;
      } else if (idle > 0) {
        return grpc.connectivityState.IDLE;
      } else if (shutdown > 0) {
        return grpc.connectivityState.SHUTDOWN;
      }

      throw new Error(
        'Cannot get connectivity state because no channel provides valid state.'
      );
    }

    /**
     * Watch for connectivity state changes.
     * @param currentState The state to watch for transitions from. This should
     *     always be populated by calling getConnectivityState immediately
     * before.
     * @param deadline A deadline for waiting for a state change
     * @param callback Called with no error when the state changes, or with an
     *     error if the deadline passes without a state change
     */
    watchConnectivityState(
      currentState: grpcType.connectivityState,
      deadline: grpcType.Deadline,
      callback: (error?: Error) => void
    ): void {
      if (!this.channelRefs.length) {
        callback(
          new Error(
            'Cannot watch connectivity state because there are no channels.'
          )
        );
        return;
      }

      const connectivityState = this.getConnectivityState(false);

      if (connectivityState !== currentState) {
        setImmediate(() => callback());
        return;
      }

      const watchState = async (channelRef: ChannelRef): Promise<void> => {
        const channel = channelRef.getChannel();
        const startingState = channel.getConnectivityState(false);

        await promisify(channel.watchConnectivityState).call(
          channel,
          startingState,
          deadline
        );

        const state = this.getConnectivityState(false);

        if (state === currentState) {
          return watchState(channelRef);
        }
      };

      const watchers = this.channelRefs.map(watchState);
      Promise.race(watchers).then(() => callback(), callback);
    }

    /**
     * Create a call object. This function will not be called when using
     * grpc.Client class. But since it's a public function of grpc.Channel,
     * It needs to be implemented for potential use cases.
     * @param method The full method string to request.
     * @param deadline The call deadline.
     * @param host A host string override for making the request.
     * @param parentCall A server call to propagate some information from.
     * @param propagateFlags A bitwise combination of elements of
     *     {@link grpc.propagate} that indicates what information to propagate
     *     from parentCall.
     * @return a grpc call object.
     */
    createCall(
      method: string,
      deadline: grpcType.Deadline,
      host: string | null,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      parentCall: any,
      propagateFlags: number | null
    ) {
      const grpcChannel = this.getChannelRef().getChannel();
      return grpcChannel.createCall(
        method,
        deadline,
        host,
        parentCall,
        propagateFlags
      );
    }
  };
}
