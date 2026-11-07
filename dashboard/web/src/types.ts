export interface Instance {
  version: string;
  listen: string;
  transport: string;
  protocol: string;
  storage: string;
  tls: boolean;
  mtls: boolean;
  auth_policy: boolean;
  enhanced_auth: boolean;
}
export interface TopologyNode {
  id: number;
  roles: string[];
  controller_id?: number | null;
  broker_id?: number | null;
  address: string;
  control_address?: string | null;
  broker_address?: string | null;
  applied: number;
  expires_at: number;
  max_lease_ms: number;
  lease_remaining_ms: number;
  clients: number;
  disk_id?: string;
  excluded: boolean;
  self: boolean;
  leader: boolean;
  online: boolean;
}
export interface TopologyRepair {
  id: string;
  target: number;
  generation: number;
  sealed: boolean;
  state: string;
}
export interface Topology {
  revision: number;
  leader: number | null;
  term: number;
  last_applied: number;
  data_storage: string;
  data_generation: number;
  clients: number;
  routes: number;
  self_node_id: number;
  nodes: TopologyNode[];
  excluded: number[];
  repair?: TopologyRepair | null;
  last_repair?: TopologyRepair | null;
}
export interface Overview {
  ready: boolean;
  uptime_seconds: number;
  online_sessions: number;
  offline_sessions: number;
  subscriptions: number;
  retained_messages: number;
  messages_in_total: number;
  messages_out_total: number;
  bytes_in_total: number;
  bytes_out_total: number;
  state_bytes: number;
  sampled_at_millis: number;
  instance: Instance;
  resources: { cpu_percent: number | null; rss_bytes: number | null };
  capabilities: {
    events: boolean;
    resource_metrics: boolean;
    mutations: boolean;
  };
  topology: Topology;
}
export interface Session {
  client_id: string;
  online: boolean;
  protocol: string;
  principal: string;
  persistent: boolean;
  subscriptions: number;
  queued: number;
  inflight: number;
  receive_maximum: number | null;
  session_expiry_interval: number;
}
export interface Subscription {
  client_id: string;
  filter: string;
  qos: number;
  no_local: boolean;
  retain_as_published: boolean;
  retain_handling: number;
  identifier: number | null;
}
export interface Retained {
  topic: string;
  qos: number;
  payload_bytes: number;
  expires_at_millis: number | null;
}
export interface Page<T> {
  items: T[];
  total: number;
  offset: number;
  limit: number;
}
export interface Configuration {
  instance: Instance;
  limits: Record<string, number>;
}
export interface Query {
  offset: number;
  limit: number;
  search: string;
  state: string;
  protocol: string;
}
export type Route =
  | "overview"
  | "connections"
  | "topics"
  | "security"
  | "listeners"
  | "configuration"
  | "logs"
  | "rules";
export interface Rate {
  at: number;
  incoming: number;
  outgoing: number;
}
