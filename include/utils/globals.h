#pragma once

// We put them in a struct/namespace so they don't conflict with other names
struct DevFlags {
    static bool LOG_ELECTION;    // Voting, Terms, State Changes
    static bool LOG_REPLICATION; // Appending Entries, Heartbeats
    static bool LOG_EXECUTION;   // Applying to State Machine (Money transfers)
    static bool LOG_NETWORK;     // Raw bytes, connection errors
    static bool LOG_CLIENT;      // Client retries and responses
    static bool LOG_SNAPSHOT;    // Snapshot creation and installation
};