/*******************************************************************************
 * FILE 15: SECURITY HARDENING - USER WHITELISTING
 *
 * Purpose: Configure network policies for user access control
 *
 * Strategy:
 * - Whitelisted users: Unrestricted access from any IP (work from anywhere)
 * - Other users: Subject to account-level or role-based network policies
 *
 * Whitelisted Users:
 * - Human users: JAKOB.KRISTENSEN, JAKOB_KRISTENSEN, JANBERTELSEN,
 *                MARTIN.SMEDEGAARD, STORM.VOLF
 * - Service accounts: SVC_ECONOMIC_TASKS, TABLEAU_READER
 *
 * Existing Network Policies (preserved):
 * - DEVOTEAM_OFFICE: Office IP restrictions (3 IPs)
 * - TABLEAU_CLOUD: Tableau Cloud IP restrictions (1 IP)
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

/*******************************************************************************
 * STEP 1: CREATE UNRESTRICTED NETWORK POLICY
 ******************************************************************************/

CREATE OR REPLACE NETWORK POLICY WHITELISTED_USERS_POLICY
    ALLOWED_IP_LIST = ('0.0.0.0/0')  -- Allow all IPv4 addresses
    COMMENT = 'Unrestricted access for whitelisted users - can access from any location (home, travel, mobile, etc.)';

/*******************************************************************************
 * STEP 2: APPLY UNRESTRICTED POLICY TO WHITELISTED USERS
 ******************************************************************************/

-- Human Users (Admins and Analysts)
ALTER USER JAKOB.KRISTENSEN SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;
ALTER USER JAKOB_KRISTENSEN SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;
ALTER USER JANBERTELSEN SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;
ALTER USER MARTIN.SMEDEGAARD SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;
ALTER USER STORM.VOLF SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;

-- Service Accounts
ALTER USER SVC_ECONOMIC_TASKS SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;
ALTER USER TABLEAU_READER SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;

/*******************************************************************************
 * STEP 3: REMOVE ACCOUNT-LEVEL NETWORK POLICY (OPTIONAL)
 ******************************************************************************/

-- If you have an account-level network policy that blocks these users, remove it
-- This ensures whitelisted users can access from anywhere
-- Uncomment if needed:
-- ALTER ACCOUNT UNSET NETWORK_POLICY;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Check all network policies
-- SHOW NETWORK POLICIES;

-- Verify whitelisted users have the correct policy
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER JAKOB.KRISTENSEN;
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER JAKOB_KRISTENSEN;
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER JANBERTELSEN;
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER MARTIN.SMEDEGAARD;
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER STORM.VOLF;
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER SVC_ECONOMIC_TASKS;
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER TABLEAU_READER;

-- List all users and their network policies
-- SELECT
--     name as user_name,
--     has_password,
--     has_rsa_public_key,
--     default_role,
--     disabled
-- FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
-- WHERE deleted_on IS NULL
-- ORDER BY name;

/*******************************************************************************
 * NOTES
 ******************************************************************************/

-- Existing network policies (DEVOTEAM_OFFICE, TABLEAU_CLOUD) remain unchanged
-- and can still be used for other users or at the account level if needed.

-- Security Benefits:
-- ✅ Whitelisted users can work from anywhere (home, travel, mobile)
-- ✅ Service accounts (tasks, Tableau) won't be blocked by IP restrictions
-- ✅ Other users can still be restricted via different policies
-- ✅ Existing office/cloud policies preserved for future use

-- To add more whitelisted users in the future:
-- ALTER USER NEW_USERNAME SET NETWORK_POLICY = WHITELISTED_USERS_POLICY;

-- To remove a user from whitelist:
-- ALTER USER USERNAME UNSET NETWORK_POLICY;

/*******************************************************************************
 * END OF FILE 15 - Security Hardening
 ******************************************************************************/
