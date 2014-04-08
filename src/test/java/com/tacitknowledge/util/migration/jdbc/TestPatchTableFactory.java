package com.tacitknowledge.util.migration.jdbc;

import com.tacitknowledge.util.migration.MigrationException;
import com.tacitknowledge.util.migration.PatchInfoStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Trivial factory to create new TestPatchTable instances.
 *
 * @author Rick Balsano (rick@opower.com)
 */
public class TestPatchTableFactory extends PatchTableFactory {
    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(TestPatchTableFactory.class);

    public PatchInfoStore createPatchTable(JdbcMigrationContext context) throws MigrationException {
        log.debug("Creating a TestPatchTable instance");
        return new TestPatchTable(context);
    }
}
