/**
 * Module dependencies
 */
var documentClient = require('documentdb').DocumentClient;
var util = require('util');
// const async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:cosmosdb-sql');

/**
 * Initialize the  connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!documentClient) {
    return;
  }

  const s = dataSource.settings;
  s.safe = s.safe !== false;

  dataSource.connector = new CosmosDB(s, dataSource);

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

exports.CosmosDB = CosmosDB;

/**
 * The constructor for CosmosDB connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function CosmosDB(settings, dataSource) {
  Connector.call(this, 'cosmosdb-sql', settings);

  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', settings);
  }

  this.dataSource = dataSource;

  this.queryOptions = {
    enableCrossPartitionQuery: settings.enableCrossPartitionQueries || false,
  };
}

util.inherits(CosmosDB, Connector);

/**
 * Connect to CosmosDB
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Client} client The cosmos client object
 */
CosmosDB.prototype.connect = function(callback) {
  var self = this;
  if (self.client) {
    process.nextTick(() => {
      callback && callback(null, self.client);
    });
  } else if (self.dataSource.connecting) {
    self.dataSource.once('connected', () => {
      process.nextTick(() => {
        callback && callback(null, self.client);
      });
    });
  } else {
    self.client = new documentClient(self.settings.endpoint, {
      masterKey: this.settings.primaryKey,
    });
    if (self.client) {
      self.getOrCreateDatabase(this.settings.database, function(err, db) {
        if (err) {
          callback && callback(err);
        }

        self.db = db;
        callback && callback(null, self.db);
      });
    } else {
      callback && callback(new Error('Failed to establish connection with endpoint'));
    }
    callback && callback(null, self.client);
  }
};

CosmosDB.prototype.disconnect = function(callback) {
  debug('Disconnecting from %j', this.settings.endpoint);
  this.client = null;
  callback(null);
};

/**
 * Get collection name for a given model
 * @param {String} model Model name
 * @returns {String} collection name
 */
CosmosDB.prototype.collectionName = function(model) {
  var modelClass = this._models[model];
  if (modelClass.settings.cosmosdb) {
    model = modelClass.settings.cosmosdb.collection || model;
  }
  return model;
};

/**
 * Get database or create new one if it doesn't exist
 * @param {String} databaseId Database name
 * @param {Function} [callback] The callback function
 */
CosmosDB.prototype.getOrCreateDatabase = function(databaseId, callback) {
  var self = this;
  var querySpec = {
    query: 'SELECT * FROM root r WHERE r.id= @id',
    parameters: [
      {
        name: '@id',
        value: databaseId,
      },
    ],
  };

  self.client.queryDatabases(querySpec).toArray(function(err, results) {
    if (err) {
      callback(err);
    } else {
      if (results.length === 0) {
        var databaseSpec = {
          id: databaseId,
        };

        self.client.createDatabase(databaseSpec, function(err, created) {
          if (err) {
            callback(err);
          }
          callback(null, created);
        });
      } else {
        callback(null, results[0]);
      }
    }
  });
};

/**
 * Get collection or create new one if it doesn't exist
 * @param {Object} databaseLink Database link object
 * @param {String} collectionId Collection name
 * @param {Function} [callback] The callback function
 */
CosmosDB.prototype.getOrCreateCollection = function(databaseLink, collectionId, callback) {
  var self = this;
  var querySpec = {
    query: 'SELECT * FROM root r WHERE r.id=@id',
    parameters: [
      {
        name: '@id',
        value: collectionId,
      },
    ],
  };

  self.client.queryCollections(databaseLink, querySpec).toArray(function(err, results) {
    if (err) {
      callback(err);
    } else {
      if (results.length === 0) {
        var collectionSpec = {
          id: collectionId,
        };

        self.client.createCollection(databaseLink, collectionSpec, {offerThroughput: 400}, function(
          err,
          created
        ) {
          if (err) {
            callback(err);
          }
          callback(null, created);
        });
      } else {
        callback(null, results[0]);
      }
    }
  });
};

/**
 * Access a CosmosDB collection by model name
 * TODO: Review if this needs to be kept
 * @param {String} model The model name
 * @returns {*}
 */
CosmosDB.prototype.collection = function(model) {
  var self = this;
  if (!self.db) {
    throw new Error(g.f('{{CosmosDB}} connection is not established'));
  }
  var collectionName = self.collectionName(model);
  return self.db.collection(collectionName);
};

/**
 * Create a new model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Object} data The request options
 * @param {Function} [callback] The callback function
 */
CosmosDB.prototype.create = function(model, data, options, callback) {
  var self = this;

  self.client.createDocument(
    buildCollectionUri(self.settings.database, self.collectionName(model)),
    data,
    options,
    function(err, document) {
      if (self.debug) {
        debug('create.callback', model, err, document);
      }
      if (err) {
        return callback(err);
      }

      debug('%j document created', document.id);
      callback(null, document.id);
    }
  );
};

/**
 * Execute a cosmosdb command
 * @param {String} model The model name
 * @param {String} command The command name
 * @param [...] params Parameters for the given command
 */
CosmosDB.prototype.execute = function(model, command, data, options, callback) {
  var self = this;

  var context = {
    model: model,
    req: {
      uri: buildCollectionUri(self.settings.database, self.collectionName(model)),
      command: command,
      data: data,
    },
    options: options,
  };

  self.notifyObserversAround(
    'execute',
    context,
    function(context, done) {
      debug('>>> Client URI details: %j', context.req.uri);
      self.client.upsertDocument(context.req.uri, context.req.data, context.options, function(
        err,
        result
      ) {
        if (err) {
          debug('Error: %j %j %j', err, context.req.uri, context.req.data);
        } else {
          context.res = result;
          debug('Result: ', result);
        }
        done(err, result);
      });
      debug('CosmosDB: model=%s command=%s', model, command);
    },
    callback
  );
};

/**
 * Save the model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Object} options The request options
 * @param {Function} [callback] The callback function
 */
CosmosDB.prototype.save = function(model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('save', model, data);
  }

  self.updateOrCreate(model, data, options, function(err, result) {
    if (self.debug) {
      debug('save.callback', model, err, result);
    }

    if (err) {
      return callback(err);
    }
    callback(null, result);
  });
};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The request options
 * @param {Function} [callback] The callback function
 */
CosmosDB.prototype.updateOrCreate = function(model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('updateOrCreate', model, data);
  }

  self.client.upsertDocument(
    buildCollectionUri(self.settings.database, self.collectionName(model)),
    data,
    options,
    function(err, result) {
      if (self.debug) {
        debug('updateOrCreate.callback', model, err, result);
      }
      if (err) {
        return callback(err);
      }
      callback(null, result);
    }
  );
};

CosmosDB.prototype.replaceOrCreate = CosmosDB.prototype.updateOrCreate;

CosmosDB.prototype.replaceById = function(model, id, data, options, callback) {
  if (id === null || id === undefined) {
    return callback(new Error('ID value is required'));
  }

  const modelDefinition = this.getModelDefinition(model);
  const collectionName = this.collectionName(model);

  this.client.replaceDocument(
    buildDocumentUri(this.settings.database, collectionName, id),
    data,
    function(error, document) {
      if (error) {
        return callback(error);
      }

      debug('%j document replaced', document.id);
      callback(null, dropNonViewProperties(modelDefinition, document));
    }
  );
};

/**
 * Find matching model instances by the filter
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Object} options The feed options
 * @param {Function} [callback] The callback function
 */
CosmosDB.prototype.all = function(model, filter, options, callback) {
  var self = this;
  if (self.debug) {
    debug('all', model, filter);
  }

  var collectionUri = buildCollectionUri(self.settings.database, self.collectionName(model));
  var modelDefinition = self.getModelDefinition(model);

  try {
    var querySpec = buildQuerySpecForModel(modelDefinition, filter);
    var iterator = self.client.queryDocuments(collectionUri, querySpec, self.queryOptions);

    iterator.toArray(function(err, documents) {
      if (err) {
        return callback(err);
      }

      callback(null, documents.map(x => dropNonViewProperties(modelDefinition, x)));
    });
  } catch (err) {
    callback(err);
  }
};

CosmosDB.prototype.count = function(model, where, options, callback) {
  const collectionUri = buildCollectionUri(this.databaseName, this.collectionName);

  try {
    const querySpec = buildQuerySpecForModel(this.getModelDefinition(model), {where: where}, [
      'COUNT(c._self) AS count',
    ]);
    const iterator = this.client.queryDocuments(collectionUri, querySpec);

    iterator.nextItem(function(error, result) {
      if (error) {
        callback(error);
      } else {
        callback(null, result.count);
      }
    });
  } catch (error) {
    callback(error);
  }
};

CosmosDB.prototype.update = function(model, where, data, options, callback) {
  const collectionUri = buildCollectionUri(this.databaseName, this.collectionName);
  const modelDefinition = this.getModelDefinition(model);

  try {
    // Translate data properties from view to DB.
    data = translateDBObjectFromView(modelDefinition, data);

    const querySpec = buildQuerySpecForModel(modelDefinition, {where: where});
    const iterator = this.client.queryDocuments(collectionUri, querySpec);

    let totalItems = 0;
    let numberOfSuccesses = 0;

    // Recursively apply update operations to all matching documents.
    // Underlying client library makes sure that all documents are
    // fetched. We just travel through the query iterator and execute
    // replace for each document in sequence. Because CosmosDB has no
    // multi-document transactions, we do not stop on individual
    // errors but rather execute the whole query iterator until end.
    // At the end, we report the ratio of how many operations succeeded
    // to caller. So that they can decide what to do.
    const next = () => {
      iterator.nextItem((error, document) => {
        if (error) {
          callback(error);
        } else if (document !== undefined) {
          // Execute replace to next item in iteration.
          ++totalItems;

          // Set updated properties to document.
          Object.assign(document, data);

          const documentUri = buildDocumentUri(this.databaseName, this.collectionName, document.id);

          this.client.replaceDocument(documentUri, document, error => {
            if (!error) {
              ++numberOfSuccesses;
            } else {
              debug('Individual replace operator failed: %j', error);
            }

            // Just move to next item, ignore possible errors.
            next();
          });
        } else {
          // Finish up the operation and report the caller.
          debug('Batch update success rate is %j', numberOfSuccesses / totalItems);
          callback(null, {
            count: numberOfSuccesses,
            successRate: numberOfSuccesses / totalItems,
          });
        }
      });
    };

    next();
  } catch (error) {
    callback(error);
  }
};

CosmosDB.prototype.destroyAll = function(model, where, options, callback) {
  const collectionUri = buildCollectionUri(this.databaseName, this.collectionName);

  try {
    const querySpec = buildQuerySpecForModel(this.getModelDefinition(model), {where: where}, [
      'c._self',
    ]);
    const iterator = this.client.queryDocuments(collectionUri, querySpec);

    let totalItems = 0;
    let numberOfSuccesses = 0;

    // Recursively apply delete operations to all matching documents.
    // Underlying client library makes sure that all documents are
    // fetched. We just travel through the query iterator and execute
    // delete for each document in sequence. Because CosmosDB has no
    // multi-document transactions, we do not stop on individual
    // errors but rather execute the whole query iterator until end.
    // At the end, we report the ratio of how many operations succeeded
    // to caller. So that they can decide what to do.
    const next = () => {
      iterator.nextItem((error, document) => {
        if (error) {
          callback(error);
        } else if (document !== undefined) {
          // Execute delete to next item in iteration.
          ++totalItems;

          this.client.deleteDocument(document._self, error => {
            if (!error) {
              ++numberOfSuccesses;
            } else {
              debug('Individual delete operator failed: %j', error);
            }

            // Just move to next item, ignore possible errors.
            next();
          });
        } else {
          // Finish up the operation and report the caller.
          debug('Batch destroy success rate is %j', numberOfSuccesses / totalItems);
          callback(null, {
            count: numberOfSuccesses,
            successRate: numberOfSuccesses / totalItems,
          });
        }
      });
    };

    next();
  } catch (error) {
    callback(error);
  }
};

CosmosDB.prototype.updateAttributes = function(model, id, data, options, cb) {
  // CosmosDB has no native support for individual attribute updates.
  // Instead we just update documents with given ID and replace them
  // with newer versions.
  this.update(model, {id: id}, data, options, cb);
};

function buildCollectionUri(databaseName, collectionName) {
  return `dbs/${databaseName}/colls/${collectionName}`;
}

function buildDocumentUri(databaseName, collectionName, documentId) {
  return `dbs/${databaseName}/colls/${collectionName}/docs/${documentId}`;
}

function dropNonViewProperties(modelDefinition, object) {
  const viewObject = {};

  for (const dbProperty in object) {
    try {
      const viewProperty = translateViewPropertyFromDB(modelDefinition, dbProperty);

      viewObject[viewProperty] = object[dbProperty];
    } catch (error) {
      // Property is dropped because it cannot be translated to view property.
    }
  }

  return viewObject;
}

function translateDBPropertyFromView(modelDefinition, viewProperty) {
  if (typeof modelDefinition !== 'object') {
    return viewProperty;
  }

  if (typeof modelDefinition.properties !== 'object') {
    return viewProperty;
  }

  var prop = modelDefinition.properties[viewProperty] || {};

  if (prop.cosmosdb) {
    viewProperty = prop.cosmosdb.propertyName || prop.cosmosdb.fieldName ||
                   prop.cosmosdb.columnName || prop.cosmosdb.column ||
                   prop.columnName || prop.column || viewProperty;
  } else {
    viewProperty = prop.columnName || prop.column || viewProperty;
  }

  return viewProperty;
}

function translateViewPropertyFromDB(modelDefinition, dbProperty) {
  for (const property in modelDefinition.properties) {
    if (
      modelDefinition.properties[property].cosmosdb !== undefined &&
      modelDefinition.properties[property].cosmosdb.propertyName === dbProperty
    ) {
      return property;
    }
  }

  if (modelDefinition.properties[dbProperty] !== undefined) {
    return dbProperty;
  }

  throw new Error(
    `'${dbProperty}' is not any of available model properties: ${Object.keys(
      modelDefinition.properties
    ).join(', ')}, or it doesn't have a valid 'cosmosdb.propertyName' configuration.`
  );
}

function translateDBObjectFromView(modelDefinition, object) {
  const dbObject = {};

  for (const viewProperty in object) {
    const dbProperty = translateDBPropertyFromView(modelDefinition, viewProperty);

    dbObject[dbProperty] = object[viewProperty];
  }

  return dbObject;
}

function isCaseInsensitive(modelDefinition, property) {
  return (
    modelDefinition.properties[property] !== undefined &&
    modelDefinition.properties[property].cosmosdb !== undefined &&
    !!modelDefinition.properties[property].cosmosdb.caseInsensitive
  );
}

function buildWhereClauses(modelDefinition, params, where) {
  return Object.keys(where).map(x => {
    const cond = where[x];
    const normalizedKey = x.toUpperCase().trim();

    // Build a top-level logical operator.
    if (['AND', 'OR'].indexOf(normalizedKey) >= 0) {
      // All sub-level logical operators are AND if nothing else is specified.
      const logicalClause = where[x]
        .map(y => buildWhereClauses(modelDefinition, params, y).join(' AND '))
        .join(` ${normalizedKey} `);
      return `(${logicalClause})`;
    }

    const dbProperty = translateDBPropertyFromView(modelDefinition, x);

    // Use CONTAINS() function to build LIKE operator since CosmosDB does not
    // support LIKEs.
    if (cond && cond['like'] !== undefined) {
      params.push(cond['like']);

      return `CONTAINS(${decorate(
        modelDefinition,
        dbProperty,
        escapeColumn('c', dbProperty)
      )}, ${decorate(modelDefinition, dbProperty, '@_' + params.length.toString())})`;
    }

    // Use CONTAINS() function to build NOT LIKE operator since CosmosDB does not
    // support LIKEs.
    if (cond && where[x]['nlike'] !== undefined) {
      params.push(cond['nlike']);
      return `NOT CONTAINS(${decorate(
        modelDefinition,
        dbProperty,
        escapeColumn('c', dbProperty)
      )}, ${decorate(modelDefinition, dbProperty, '@_' + params.length.toString())})`;
    }

    // Use CONTAINS() and LOWER() functions to build case-insensitive LIKE
    // operator since CosmosDB does not support case-insensitive LIKEs.
    if (cond && cond['ilike'] !== undefined) {
      params.push(cond['ilike']);
      return `CONTAINS(LOWER(${escapeColumn('c', dbProperty)}), LOWER(@_${params.length}))`;
    }

    // Use CONTAINS() and LOWER() functions to build case-insensitive NOT LIKE
    // operator since CosmosDB does not support case-insensitive LIKEs.
    if (cond && cond['nilike'] !== undefined) {
      params.push(cond['nilike']);
      return `NOT CONTAINS(LOWER(${escapeColumn('c', dbProperty)}), LOWER(@_${params.length}))`;
    }

    // Build greater than operator.
    if (cond && cond['gt'] !== undefined) {
      params.push(cond['gt']);
      return `${decorate(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} > ${decorate(
        modelDefinition,
        dbProperty,
        '@_' + params.length.toString()
      )}`;
    }

    // Build greater than or equal operator.
    if (cond && cond['gte'] !== undefined) {
      params.push(cond['gte']);
      return `${decorate(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} >= ${decorate(
        modelDefinition,
        dbProperty,
        '@_' + params.length.toString()
      )}`;
    }

    // Build less than operator.
    if (cond && cond['lt'] !== undefined) {
      params.push(cond['lt']);
      return `${decorate(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} < ${decorate(
        modelDefinition,
        dbProperty,
        '@_' + params.length.toString()
      )}`;
    }

    // Build less than or equal operator.
    if (cond && cond['lte'] !== undefined) {
      params.push(cond['lte']);
      return `${decorate(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} <= ${decorate(
        modelDefinition,
        dbProperty,
        '@_' + params.length.toString()
      )}`;
    }

    // Build IN operator.
    if (cond && cond['inq'] !== undefined) {
      const positions = [];

      for (let i = 0; i < cond['inq'].length; ++i) {
        params.push(cond['inq'][i]);
        positions.push(params.length);
      }

      const inParams = positions.map(i => decorate(modelDefinition, dbProperty, `@_${i}`));

      return `${decorate(
        modelDefinition,
        dbProperty,
        escapeColumn('c', dbProperty)
      )} IN (${inParams.join(',')})`;
    }

    // Build NOT IN operator.
    if (cond && cond['nin'] !== undefined) {
      const positions = [];

      for (let i = 0; i < cond['nin'].length; ++i) {
        params.push(cond['nin'][i]);
        positions.push(params.length);
      }

      const inParams = positions.map(i => decorate(modelDefinition, dbProperty, `@_${i}`));

      return `${decorate(
        modelDefinition,
        dbProperty,
        escapeColumn('c', dbProperty)
      )} NOT IN (${inParams.join(',')})`;
    }

    // Build non-equality operator.
    if (cond && cond['neq'] !== undefined) {
      params.push(cond['neq']);
      return `${decorate(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} <> ${decorate(
        modelDefinition,
        dbProperty,
        '@_' + params.length.toString()
      )}`;
    }

    // Build BETWEEN operator.
    if (cond && cond['between'] !== undefined) {
      if (cond['between'].length !== 2) {
        throw new Error(
          `'between' operator has incorrect number of parameters. It should have exactly 2, but has ${
            where[x]['between'].length
          }.`
        );
      }

      params.push(cond['between'][0]);
      params.push(cond['between'][1]);
      return `(${decorate(
        modelDefinition,
        dbProperty,
        escapeColumn('c', dbProperty)
      )} BETWEEN @_${params.length - 1} AND ${decorate(
        modelDefinition,
        dbProperty,
        '@_' + params.length.toString()
      )})`;
    }

    // Use ARRAY_CONTAINS to build any operator.
    if (cond && cond['any'] !== undefined) {
      params.push(cond['any']);
      return `ARRAY_CONTAINS(${decorate(
        modelDefinition,
        dbProperty,
        escapeColumn('c', dbProperty)
      )}, ${decorate(modelDefinition, dbProperty, '@_' + params.length.toString())})`;
    }

    // By default, assume equality operator.
    params.push(cond);
    return `${decorate(modelDefinition, dbProperty, escapeColumn('c', dbProperty))} = ${decorate(
      modelDefinition,
      dbProperty,
      '@_' + params.length.toString()
    )}`;
  });
}

function decorate(modelDefinition, dbProperty, value) {
  if (isCaseInsensitive(modelDefinition, dbProperty)) {
    return `LOWER(${value})`;
  }
  return value;
}

function escapeColumn(row, dbProperty) {
  return `${row}["${dbProperty}"]`;
}

function buildQuerySpecForModel(modelDefinition, filter, select, orderBy) {
  filter = filter || {};

  const modelProperties = Object.keys(modelDefinition.properties);
  const querySelect =
    select ||
    modelProperties.map(x => escapeColumn('c', translateDBPropertyFromView(modelDefinition, x)));

  // Set default ordering.
  let queryOrderBy = orderBy || ['c._ts'];

  // Build ordering if it is set in filters.
  if (filter.order) {
    filter.order = Array.isArray(filter.order) ? filter.order : [filter.order];

    queryOrderBy = (filter.order || []).map(x => {
      const order = x.split(' ', 2);
      const dbProperty = translateDBPropertyFromView(modelDefinition, order[0]);

      // Normalize order by type if given.
      if (order.length > 1) {
        order[1] = order[1].toUpperCase().trim();
      } else {
        // Set default order by type.
        order.push('ASC');
      }

      if (['ASC', 'DESC'].indexOf(order[1]) < 0) {
        throw new Error(`Order by '${order[1]}' is not allowed for the field '${order[0]}'.`);
      }

      return `${escapeColumn('c', dbProperty)} ${order[1]}`;
    });
  }

  const queryParams = [];
  const queryWhere = buildWhereClauses(modelDefinition, queryParams, filter.where || {});

  const querySpec = {
    query: `SELECT${isFinite(filter.limit) ? ' TOP ' + filter.limit : ''} ${querySelect.join(
      ','
    )} FROM c ${queryWhere.length > 0 ? 'WHERE ' : ''}${queryWhere.join(
      ' AND '
    )} ORDER BY ${queryOrderBy.join(',')}`,
    parameters: queryParams.map((x, i) => ({
      name: `@_${i + 1}`,
      value: x,
    })),
  };

  debug('SQL: %s, params: %j', querySpec.query, querySpec.parameters);

  return querySpec;
}
