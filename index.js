'use strict';
const PG = require('pg');
const { serializeError } = require('serialize-error');

const DEFAULT_PG_POOL_OPTIONS = {
    connectionTimeoutMillis: 0,
    idleTimeoutMillis: 3600000, // 1 hour
    max: 10,
    allowExitOnIdle: false,
};

const DEFAULT_STEPS_TABLE = 'steps';

const STATUS_NEW = 'N';
const STATUS_RUNNING = 'R';
const STATUS_DONE = 'D';
const STATUS_FAILED = 'F';

const make = (options) => {

    const pgPoolOptions = {DEFAULT_PG_POOL_OPTIONS, ...options?.pool, ...options?.client};
    const tableName = options?.table?.name || DEFAULT_STEPS_TABLE;
    const pool = new PG.Pool(pgPoolOptions);

    const getStepRow = async (client, name, hash) => {
        const [row] = await client.query(`SELECT * FROM "${tableName}" WHERE name = $1 AND hash = $2`, [name, hash]);
        return row;
    };

    const getOrCreateStepRow = async (client, name, hash, rootHash) => {
        let row = await getStepRow(client, name, hash);
        if (row) {
            row = {
                name,
                hash,
                rootHash,
                status: STATUS_NEW,
            };

            const insert = await client.query(
                `INSERT INTO "${tableName} (name, hash, rootHash, status) VALUES ($1, $2, $3, $4) RETURNING (id)`,
                [row.name, row.hash, row.rootHash, row.status]
            );

            row.id = insert.id;
        }

        return row;
    };

    const updateStatus = async (client, row, status) => {
        if (row?.id) {
            return await client.query(`UPDATE ${tableName} SET status = $2 WHERE id = $1`, [row.id, status]);
        }

        return false;
    };

    /**
     * Get existing step run info or create new one
     *
     * @param {string} name Full name of step
     * @param {string} hash Hash of step input data
     * @param {string?} rootHash Root step hash for substep. For root step should be null. If undefined the run object returned in read-only mode.
     * @returns {}
     */
    const getRun = async (name, hash, rootHash) => {

        const client = await pool.connect();
        const row = await getOrCreateStepRow(client, name, hash, rootHash);
        await client.release();

        return {
            /**
             * Check if step run is done successfully
             * @returns {boolean}
             */
            isDone() {
                return row.status === STATUS_DONE;
            },

            /**
             * Check if step run is running now
             * @returns {boolean}
             */
            isRunning() {
                return row.status === STATUS_RUNNING;
            },

            /**
             * Check if step run is failed
             * @returns {boolean}
             */
            isFailed() {
                return row.status === STATUS_FAILED;
            },

            /**
             * Mark step as running
             * @returns {boolean}
             */
            async markRunning() {
                if (row?.id && rootHash !==undefined) {
                    return await client.query(
                        `UPDATE ${tableName} SET status = $2 WHERE id = $1`,
                        [row.id, STATUS_RUNNING]
                    );
                }

                return false;
            },

            /**
             * Mark step run as successfully done
             * @param {object} output Step execution output result
             * @returns {boolean}
             */
            async markDone(output) {
                if (row?.id && rootHash !==undefined) {
                    return await client.query(
                        `UPDATE ${tableName} SET status = $2, output = $3, vars = $4, error = null WHERE id = $1`,
                        [row.id, STATUS_DONE, output, row?.vars ? JSON.stringify(row.vars) : null]
                    );
                }

                return false;
            },

            /**
             * Mark step run as failed
             * @param {Error} error Error happend while executing step
             * @returns {boolean}
             */
            async markFailed(error) {
                if (row?.id && rootHash !==undefined) {
                    return await client.query(
                        `UPDATE ${tableName} SET status = $2, error = $3, output = null WHERE id = $1`,
                        [row.id, STATUS_FAILED, serializeError(error)]
                    );
                }

                return false;
            },

            /**
             * Get step run persistent variable
             * @returns {object}
             */
            getVars() {
                if (typeof row?.vars === 'string') {
                    row.vars = JSON.parse(row.vars);
                } else {
                    row.vars = {};
                }

                return row.vars;
            },
        };
    };

    return {
        getRun,
    };
};

module.exports = {
    make,
};
