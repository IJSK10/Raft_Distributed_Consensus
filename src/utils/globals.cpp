#include "utils/globals.h"

// DEFAULT SETTINGS
// Set these to 'true' or 'false' depending on what you want to see
bool DevFlags::LOG_ELECTION    = true; 
bool DevFlags::LOG_REPLICATION = false; // Usually the most noisy
bool DevFlags::LOG_EXECUTION   = true;
bool DevFlags::LOG_NETWORK     = false;
bool DevFlags::LOG_CLIENT      = false;
bool DevFlags::LOG_SNAPSHOT    = true;