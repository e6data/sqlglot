import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

// Test configuration
export const options = {
  stages: [
    { duration: '1m', target: 2 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<2000'], // 95% of requests under 500ms, 99% under 2s
    errors: ['rate<0.01'], // Error rate under 1%
    http_req_failed: ['rate<0.01'], // Failed requests under 1%
  },
};

// Test data - sample queries
const queries = {
  simple: 'SELECT * FROM users',
  withWhere: 'SELECT * FROM users WHERE id > 100 AND created_at > \'2024-01-01\'',
  withJoin: 'SELECT u.id, u.name, o.order_id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 1000',
  withCTE: `WITH user_orders AS (
    SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent
    FROM orders
    GROUP BY user_id
  )
  SELECT u.*, uo.order_count, uo.total_spent
  FROM users u
  JOIN user_orders uo ON u.id = uo.user_id
  WHERE uo.order_count > 5`,
  complex: `WITH active_users AS (
    SELECT id, name, email, created_at
    FROM users
    WHERE status = 'active' AND created_at > '2023-01-01'
  ),
  user_metrics AS (
    SELECT
      user_id,
      COUNT(*) as total_orders,
      SUM(total) as total_spent,
      AVG(total) as avg_order_value,
      MAX(created_at) as last_order_date
    FROM orders
    WHERE status = 'completed'
    GROUP BY user_id
    HAVING COUNT(*) > 3
  )
  SELECT
    au.id,
    au.name,
    au.email,
    um.total_orders,
    um.total_spent,
    um.avg_order_value,
    um.last_order_date,
    DATEDIFF(day, um.last_order_date, CURRENT_DATE) as days_since_last_order
  FROM active_users au
  LEFT JOIN user_metrics um ON au.id = um.user_id
  WHERE um.total_spent > 5000
  ORDER BY um.total_spent DESC
  LIMIT 100`,
};

const dialects = ['databricks', 'snowflake'];

const BASE_URL = __ENV.API_URL || 'http://localhost:8100';

export default function () {
  // Pick a random query type
  const queryTypes = Object.keys(queries);
  const queryType = queryTypes[Math.floor(Math.random() * queryTypes.length)];
  const query = queries[queryType];

  // Pick a random dialect
  const dialect = dialects[Math.floor(Math.random() * dialects.length)];

  const payload = JSON.stringify({
    query: query,
    from_sql: dialect,
    to_sql: 'e6',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: { query_type: queryType, dialect: dialect },
  };

  // Make the request
  const response = http.post(`${BASE_URL}/api/v1/inline/transpile`, payload, params);

  // Check response
  const result = check(response, {
    'status is 200': (r) => r.status === 200,
    'response has transpiled_query': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.transpiled_query !== undefined && body.transpiled_query.length > 0;
      } catch (e) {
        return false;
      }
    },
    'response time OK': (r) => r.timings.duration < 5000, // 5 second timeout
  });

  // Record errors
  errorRate.add(!result);
}
