'use strict';
const PG = require('pg');

const DEFAULT_PG_POOL_OPTIONS = {
    connectionTimeoutMillis: 0,
    idleTimeoutMillis: 3600000, // 1 hour
    max: 5,
    allowExitOnIdle: false,
    database: 'steps',
};

const DEFAULT_STEPS_TABLE = 'steps';

const STATUS_NEW = 'N';
const STATUS_RUNNING = 'R';
const STATUS_DONE = 'D';
const STATUS_FAILED = 'F';

const serializeError = (error) => {
    return JSON.stringify(error, Object.getOwnPropertyNames(error));
};

const now = () => {
    return (new Date).toISOString();
};

const make = (options) => {

    const hostname = process.env.HOSTNAME || null;

    const pgPoolOptions = {...DEFAULT_PG_POOL_OPTIONS, ...options?.pool, ...options?.client};
    const tableName = options?.table?.name || DEFAULT_STEPS_TABLE;
    const pool = new PG.Pool(pgPoolOptions);

    const activeRuns = new Map();
    const addActiveRun = (hash, run) => activeRuns.set(hash, run);
    const removeActiveRun = (hash) => activeRuns.delete(hash);

    const finishActiveRuns = async () => {
        const error = new Error('Exit');
        const finish = [];
        for(const [hash, run] of activeRuns.entries()) {
            finish.push(run.markFailed(error));
        }

        return await Promise.all(finish);
    };

    process.on('beforeExit', finishActiveRuns);

    const getStepRow = async (client, name, hash) => {
        const result = await client.query(`SELECT * FROM "${tableName}" WHERE name = $1 AND hash = $2`, [name, hash]);
        const row = result.rows[0];
        return row;
    };

    const getOrCreateStepRow = async (client, name, hash, rootHash) => {
        let row = await getStepRow(client, name, hash);

        if (!row) {
            row = {
                name,
                hash,
                rootHash,
                status: STATUS_NEW,
            };

            const insert = await client.query(
                `INSERT INTO ${tableName} (name, hash, rootHash, status) VALUES ($1, $2, $3, $4) RETURNING (id)`,
                [row.name, row.hash, row.rootHash, row.status]
            );

            row.id = insert.rows[0].id;
        }

        return row;
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
        const row = rootHash ? await getOrCreateStepRow(client, name, hash, rootHash) : await getStepRow(client, name, hash);
        await client.release();

        const run = {
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
                    addActiveRun(hash, run);
                    const client = await pool.connect();
                    try {
                        const result = await client.query(
                            `UPDATE ${tableName} SET status = $2, vars = $3, hostname = $4, started_at = $5 WHERE id = $1`,
                            [row.id, STATUS_RUNNING, row?.vars ? JSON.stringify(row.vars) : null, hostname, now()]
                        );

                        return result.rowCount;
                    } finally {
                        if (client) {
                            await client.release();
                        }
                    }
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
                    removeActiveRun(hash);
                    const client = await pool.connect();
                    try {
                        const result = await client.query(
                            `UPDATE ${tableName} SET status = $2, output = $3, vars = $4, error = null WHERE id = $1`,
                            [
                                row.id, STATUS_DONE,
                                output ? JSON.stringify(output) : null,
                                row?.vars ? JSON.stringify(row.vars) : null
                            ]
                        );


                        return result.rowCount;
                    } finally {
                        if (client) {
                            await client.release();
                        }
                    }
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
                    removeActiveRun(hash);
                    const client = await pool.connect();
                    try {
                        const result = await client.query(
                            `UPDATE ${tableName} SET status = $2, error = $3, vars = $4, output = null WHERE id = $1`,
                            [row.id, STATUS_FAILED, serializeError(error), row?.vars ? JSON.stringify(row.vars) : null]
                        );

                        return result.rowCount;
                    } finally {
                        if (client) {
                            await client.release();
                        }
                    }
                }

                return false;
            },

            /**
             * Get step run output
             * @returns {object}
             */
            getOutput() {
                return row.output;
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

        return run;
    };

    return {
        getRun,
    };
};

module.exports = {
    make,
};
