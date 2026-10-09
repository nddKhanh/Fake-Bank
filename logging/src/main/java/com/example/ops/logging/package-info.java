/**
 * Observation/logging extension point, separate from the CRUD backend.
 * TODO: log querying/correlation using requestId and transferId.
 * No collector, storage or logging business implementation is supplied.
 * Normal runtime logs remain local to each application's process.
 */
package com.example.ops.logging;
