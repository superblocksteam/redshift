import {
  Column,
  DatasourceMetadataDto,
  ExecutionOutput,
  IntegrationError,
  RawRequest,
  RedshiftActionConfiguration,
  RedshiftDatasourceConfiguration,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  DatabasePlugin,
  normalizeTableColumnNames,
  PluginExecutionProps,
  CreateConnection,
  DestroyConnection
} from '@superblocksteam/shared-backend';
import { groupBy, isEmpty } from 'lodash';
import { Client, Notification } from 'pg';
import { NoticeMessage } from 'pg-protocol/dist/messages';

const TEST_CONNECTION_TIMEOUT = 5000;
const DEFAULT_SCHEMA = 'public';

export default class RedshiftPlugin extends DatabasePlugin {
  constructor() {
    super({ useOrderedParameters: true });
  }

  public async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<RedshiftDatasourceConfiguration>): Promise<ExecutionOutput> {
    const client = await this.createConnection(datasourceConfiguration);
    try {
      const ret = new ExecutionOutput();
      const query = actionConfiguration.body;
      if (!query || isEmpty(query)) {
        return ret;
      }
      const rows = await this.executeQuery(() => {
        return client.query(query, context.preparedStatementContext);
      });
      ret.output = normalizeTableColumnNames(rows.rows);
      return ret;
    } catch (err) {
      throw new IntegrationError(`Redshift query failed, ${err.message}`);
    } finally {
      if (client) {
        this.destroyConnection(client).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }

  public getRequest(actionConfiguration: RedshiftActionConfiguration): RawRequest {
    return actionConfiguration.body;
  }

  public dynamicProperties(): string[] {
    return ['body'];
  }

  public async metadata(datasourceConfiguration: RedshiftDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    const client = await this.createConnection(datasourceConfiguration);
    if (!datasourceConfiguration) {
      throw new IntegrationError('Datasource configuration not specified for Redshift step');
    }
    const schema = datasourceConfiguration.authentication?.custom?.databaseSchema?.value ?? DEFAULT_SCHEMA;
    try {
      const schemaQuery = `SELECT * FROM pg_table_def WHERE schemaname = '${schema}'`;
      const schemaResult = await this.executeQuery(() => {
        return client.query(schemaQuery);
      });

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const tableMapping = groupBy(schemaResult.rows, (result: any) => result.tablename);
      const tables = Object.keys(tableMapping).map(
        (tableName): Table => {
          return {
            name: tableName,
            type: TableType.TABLE,
            columns: tableMapping[tableName].map(
              (column): Column => {
                return { name: column.column, type: column.type };
              }
            )
          };
        }
      );
      return {
        dbSchema: {
          tables: tables
        }
      };
    } catch (err) {
      throw new IntegrationError(`Failed to connect to Redshift, ${err.message}`);
    } finally {
      if (client) {
        this.destroyConnection(client).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }

  @DestroyConnection
  private async destroyConnection(connection: Client): Promise<void> {
    await connection.end();
  }

  @CreateConnection
  private async createConnection(
    datasourceConfiguration: RedshiftDatasourceConfiguration,
    connectionTimeoutMillis = 30000
  ): Promise<Client> {
    try {
      const endpoint = datasourceConfiguration.endpoint;
      if (!endpoint) {
        throw new IntegrationError('Endpoint not specified for Redshift step');
      }
      const auth = datasourceConfiguration.authentication;
      if (!auth) {
        throw new IntegrationError('Auth not specified for Redshift step');
      }
      if (!auth.custom?.databaseName?.value) {
        throw new IntegrationError('Database name not specified for Redshift step');
      }
      const client = new Client({
        host: endpoint.host,
        user: auth.username,
        password: auth.password,
        database: auth.custom.databaseName.value,
        port: endpoint.port,
        ssl: datasourceConfiguration.connection?.useSsl ? { rejectUnauthorized: false } : false,
        connectionTimeoutMillis: connectionTimeoutMillis
      });
      this.attachLoggerToClient(client, datasourceConfiguration);

      await client.connect();
      this.logger.debug(`Redshift client connected. ${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`);
      return client;
    } catch (err) {
      throw new IntegrationError(`Failed to connect to Redshift, ${err.message}`);
    }
  }

  private attachLoggerToClient(client: Client, datasourceConfiguration: RedshiftDatasourceConfiguration) {
    if (!datasourceConfiguration) {
      return;
    }
    const datasourceEndpoint = `${datasourceConfiguration.endpoint?.host}:${datasourceConfiguration.endpoint?.port}`;

    client.on('error', (err: Error) => {
      this.logger.error(`Redshift client error. ${datasourceEndpoint} err.stack`);
    });

    client.on('end', () => {
      this.logger.debug(`Redshift client disconnected from server. ${datasourceEndpoint}`);
    });

    client.on('notification', (message: Notification): void => {
      this.logger.debug(`Redshift notification ${message}. ${datasourceEndpoint}`);
    });

    client.on('notice', (notice: NoticeMessage) => {
      this.logger.debug(`Redshift notice: ${notice.message}. ${datasourceEndpoint}`);
    });
  }

  public async test(datasourceConfiguration: RedshiftDatasourceConfiguration): Promise<void> {
    const client = await this.createConnection(datasourceConfiguration, TEST_CONNECTION_TIMEOUT);
    try {
      await this.executeQuery(() => {
        return client.query('SELECT NOW()');
      });
    } catch (err) {
      throw new IntegrationError(`Test Redshift connection failed, ${err.message}`);
    } finally {
      if (client) {
        this.destroyConnection(client).catch(() => {
          // Error handling is done in the decorator
        });
      }
    }
  }
}
