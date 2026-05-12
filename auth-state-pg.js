const { proto, initAuthCreds, BufferJSON } = require('@whiskeysockets/baileys');

async function usePostgresAuthState(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS baileys_auth (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  const readData = async (key) => {
    try {
      const result = await pool.query('SELECT value FROM baileys_auth WHERE key = $1', [key]);
      if (result.rows.length === 0) return null;
      return JSON.parse(result.rows[0].value, BufferJSON.reviver);
    } catch (err) {
      console.error('PG read error', key, err.message);
      return null;
    }
  };

  const writeData = async (key, value) => {
    try {
      const serialized = JSON.stringify(value, BufferJSON.replacer);
      await pool.query(
        `INSERT INTO baileys_auth (key, value, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
        [key, serialized]
      );
    } catch (err) {
      console.error('PG write error', key, err.message);
    }
  };

  const removeData = async (key) => {
    try {
      await pool.query('DELETE FROM baileys_auth WHERE key = $1', [key]);
    } catch (err) {
      console.error('PG delete error', key, err.message);
    }
  };

  const creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              tasks.push(value ? writeData(key, value) : removeData(key));
            }
          }
          await Promise.all(tasks);
        }
      }
    },
    saveCreds: async () => {
      await writeData('creds', creds);
    }
  };
}

module.exports = { usePostgresAuthState };
