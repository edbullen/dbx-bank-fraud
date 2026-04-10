export interface ScoredTransaction {
  id: string;
  timestamp: string;
  type: string;
  amount: number;
  customer_id: number;
  firstname: string;
  lastname: string;
  country_orig: string;
  country_dest: string;
  fraud_prediction: boolean;
  fraud_probability: number;
  scored_at: string;
}

export interface Metrics {
  total_transactions: number;
  fraud_count: number;
  legit_count: number;
  fraud_rate: number;
  total_amount: number;
}

export interface CountryFlow {
  source: string;
  target: string;
  total_count: number;
  fraud_count: number;
  total_amount: number;
}

export interface TimeBucket {
  timestamp: number;
  total: number;
  fraud: number;
  legit: number;
}

export interface GeneratorStatus {
  running: boolean;
  tps: number;
  total_generated: number;
}
