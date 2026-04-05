// usqldb -- PostgreSQL 17-compatible catalog layer for UQA
// Copyright (c) 2023-2026 Cognica, Inc.

// Module-level connection registry for pg_stat_activity.
//
// PGWireConnection instances register themselves here after
// authentication and unregister on close.  The pg_catalog builder
// reads the registry to produce live pg_stat_activity rows.

export interface ConnectionInfo {
  pid: number;
  username: string;
  database: string;
  applicationName: string;
  clientAddr: string | null;
  clientPort: number;
  backendStart: Date | null;
  xactStart: Date | null;
  queryStart: Date | null;
  stateChange: Date | null;
  state: string;
  query: string;
  backendType: string;
}

const _connections: Map<number, ConnectionInfo> = new Map();

export function registerConnection(info: ConnectionInfo): void {
  _connections.set(info.pid, info);
}

export function unregisterConnection(pid: number): void {
  _connections.delete(pid);
}

export function getAllConnections(): ConnectionInfo[] {
  return Array.from(_connections.values());
}
