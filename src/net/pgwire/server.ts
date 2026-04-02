// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// TCP-based PostgreSQL wire protocol server.
//
// PGWireServer manages the TCP listener, connection lifecycle,
// and cancel-request routing.  Each accepted connection is handled by
// a PGWireConnection running as an independent async task.
//
// Usage:
//
//     import { PGWireServer, createConfig } from "./net/pgwire/index.js";
//
//     const config = createConfig({ host: "0.0.0.0", port: 5432 });
//     const server = new PGWireServer(config);
//     await server.start();

import * as crypto from "node:crypto";
import * as net from "node:net";
import * as tls from "node:tls";
import * as fs from "node:fs";

import { USQLEngine } from "../../core/engine.js";
import { PGWireConnection } from "./connection.js";

import type { PGWireConfig } from "./config.js";

export class PGWireServer {
  private readonly _config: PGWireConfig;
  private _server: net.Server | null;
  private readonly _connections: Map<number, PGWireConnection>;
  private _nextPid: number;
  private readonly _secureContext: tls.SecureContext | null;

  constructor(config: PGWireConfig) {
    this._config = config;
    this._server = null;
    this._connections = new Map();
    this._nextPid = 1;
    this._secureContext = null;

    if (config.sslCertfile && config.sslKeyfile) {
      this._secureContext = tls.createSecureContext({
        cert: fs.readFileSync(config.sslCertfile, "utf-8"),
        key: fs.readFileSync(config.sslKeyfile, "utf-8"),
      });
    }
  }

  /** Return the actual listening port (useful when port=0). */
  get port(): number {
    if (this._server !== null) {
      const addr = this._server.address();
      if (addr !== null && typeof addr !== "string") {
        return addr.port;
      }
    }
    return this._config.port;
  }

  get host(): string {
    return this._config.host;
  }

  // ==================================================================
  // Lifecycle
  // ==================================================================

  /** Start the TCP listener. */
  async start(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const server = net.createServer((socket) => {
        this._handleClient(socket);
      });

      server.on("error", (err) => {
        reject(err);
      });

      server.listen(this._config.port, this._config.host, () => {
        this._server = server;
        resolve();
      });
    });
  }

  /** Gracefully shut down the server and all connections. */
  async stop(): Promise<void> {
    // Destroy all active connections first (otherwise server.close() hangs
    // waiting for connections to finish).
    for (const conn of this._connections.values()) {
      try {
        conn.cancel();
      } catch {
        // Ignore cancel errors
      }
    }
    this._connections.clear();

    if (this._server !== null) {
      await new Promise<void>((resolve) => {
        this._server!.close(() => {
          resolve();
        });
      });
      this._server = null;
    }
  }

  /** Start the server and run until interrupted. */
  async serveForever(): Promise<void> {
    await this.start();
    // Keep the process alive until externally stopped.
    return new Promise<void>((resolve) => {
      const onClose = (): void => {
        resolve();
      };
      if (this._server !== null) {
        this._server.on("close", onClose);
      } else {
        resolve();
      }
    });
  }

  // ==================================================================
  // Connection handling
  // ==================================================================

  private _handleClient(socket: net.Socket): void {
    // Check max connections.
    if (this._connections.size >= this._config.maxConnections) {
      socket.destroy();
      return;
    }

    const [pid, secret] = this._allocateProcessId();
    const engine = this._createEngine();

    const conn = new PGWireConnection(socket, engine as USQLEngine, {
      authMethod: this._config.authMethod,
      credentials: this._config.credentials,
      processId: pid,
      secretKey: secret,
      cancelCallback: (processId: number, secretKey: number) => {
        this._cancelQuery(processId, secretKey);
      },
      secureContext: this._secureContext,
    });
    this._connections.set(pid, conn);

    // Run the connection and clean up when done.
    conn.run().finally(() => {
      this._connections.delete(pid);
    });
  }

  private _allocateProcessId(): [number, number] {
    const pid = this._nextPid + (crypto.randomBytes(4).readUInt32BE(0) >>> 1);
    this._nextPid++;
    const secret = crypto.randomBytes(4).readUInt32BE(0) >>> 1;
    return [pid, secret];
  }

  private _createEngine(): unknown {
    if (this._config.engineFactory !== null) {
      return this._config.engineFactory();
    }
    return new USQLEngine(
      this._config.dbPath !== null ? { dbPath: this._config.dbPath } : undefined,
    );
  }

  private _cancelQuery(processId: number, secretKey: number): void {
    const conn = this._connections.get(processId);
    if (conn !== undefined && conn.secretKey === secretKey) {
      conn.cancel();
    }
  }
}
