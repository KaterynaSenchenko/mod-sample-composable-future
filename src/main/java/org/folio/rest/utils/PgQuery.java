package org.folio.rest.utils;

import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.cql.CQLWrapper;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;
import org.z3950.zing.cql.cql2pgjson.FieldException;

import java.util.Arrays;
import java.util.List;

public class PgQuery {
    private final String[] fields;
    private final String table;
    private final String query;
    private final int offset;
    private final int limit;
    private final CQLWrapper cql;

    private PgQuery(PgQueryBuilder builder) {
        this.fields = builder.fields;
        this.table = builder.table;
        this.query = builder.query;
        this.offset = builder.offset;
        this.limit = builder.limit;
        this.cql = builder.cql;
    }

    public String[] getFields() {
        return this.fields;
    }

    public String getTable() {
        return this.table;
    }

    public String getQuery() {
        return this.query;
    }

    public int getOffset() {
        return this.offset;
    }

    public int getLimit() {
        return this.limit;
    }

    public CQLWrapper getCql() {
        return this.cql;
    }

    public static class PgQueryBuilder {
        private final String[] fields;
        private final String table;
        private String query = null;
        private int offset = 0;
        private int limit = 0;
        private CQLWrapper cql;

        public PgQueryBuilder(String[] fields, String table) {
            this.fields = fields;
            this.table = table;
        }

        public PgQueryBuilder query(String query) {
            this.query = query;
            return this;
        }

        public PgQueryBuilder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public PgQueryBuilder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public PgQuery build() throws FieldException {
            List<String> jsonbFields = Arrays.asList(this.table + ".jsonb");
            CQL2PgJSON cql2PgJson = new CQL2PgJSON(jsonbFields);
            this.cql = new CQLWrapper(cql2PgJson, this.query)
                .setLimit(new Limit(this.limit))
                .setOffset(new Offset(this.offset));
            return new PgQuery(this);
        }
    }
}
