/* Copyright 2004 Tacit Knowledge
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tacitknowledge.util.migration.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.BooleanUtils;
import org.easymock.classextension.IMocksControl;

import com.mockrunner.jdbc.JDBCTestCaseAdapter;
import com.mockrunner.jdbc.PreparedStatementResultSetHandler;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.tacitknowledge.util.migration.MigrationException;
import com.tacitknowledge.util.migration.jdbc.util.ConnectionWrapperDataSource;

import static org.easymock.classextension.EasyMock.createStrictControl;
import static org.easymock.EasyMock.expect;
/**
 * Out-of-container tests the <code>PatchTable</code> class using a
 * mock JDBC driver. 
 * 
 * @author  Scott Askew (scott@tacitknowledge.com)
 */
public class PatchTableTest extends JDBCTestCaseAdapter
{
    /**
     * The <code>PatchTable</code> to test
     */
    private PatchTable table = null; 

    /**
     * The mock JDBC connection to use during the tests
     */
    private MockConnection conn = null;
    
    /**
     * The <code>JDBCMigrationConteext</code> used for testing
     */
    private DataSourceMigrationContext context = new DataSourceMigrationContext(); 

    /** Used to specify different statements in the tests */
    private PreparedStatementResultSetHandler handler = null;
    private IMocksControl contextControl;
    private JdbcMigrationContext mockContext;


    /**
     * Constructor for PatchTableTest.
     *
     * @param name the name of the test to run
     */
    public PatchTableTest(String name)
    {
        super(name);
    }
    
    /**
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {
        super.setUp();
        
        conn = getJDBCMockObjectFactory().getMockConnection();
        
        context = new DataSourceMigrationContext();
        context.setDataSource(new ConnectionWrapperDataSource(conn));
        context.setSystemName("milestone");
        context.setDatabaseType(new DatabaseType("hsqldb"));
        
        contextControl = createStrictControl();
        mockContext = contextControl.createMock(JdbcMigrationContext.class);
    }
    
    /**
     * Ensures that the class throws an <code>IllegalArgumentException</code>
     * if an unknown database type is specified in the constructor. 
     */
    public void testUnknownDatabaseType()
    {
        try
        {
            context.setDatabaseType(new DatabaseType("bad-database-type"));
            new PatchTable(context);
            fail("Expected IllegalArgumentException because of unknown database type");
        }
        catch (IllegalArgumentException e)
        {
            // Expected
        }
        catch (MigrationException e)
        {
            fail("Received an exception instantiating PatchTable:" + e.toString());
        }
    }
    
    /**
     * Validates the automatic creation of the patches table.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testCreatePatchesTable() throws Exception
    {
        // Test-specific setup
        PreparedStatementResultSetHandler h = conn.getPreparedStatementResultSetHandler();
        h.prepareThrowsSQLException(context.getSql(DatabaseType.PATCH_TABLE_EXISTS_STATEMENT_KEY));
        
        setupLevelCreatedMock(h, false);

        commonVerifications();
        verifyCommitted();
        verifyPreparedStatementParameter(0, 1, "milestone");
        verifySQLStatementExecuted(context.getSql(DatabaseType.CREATE_PATCH_TABLE_STATEMENT_KEY));
    }
    
    /**
     * Tests when trying to get a connection to the database to create the patch table fails.
     * @throws SQLException shouldn't occur, only declared to make the code below more readable.
     */
    public void testCreatePatchesTableWithoutConnection() throws SQLException
    {
        // setup mock calls
        expect(mockContext.getDatabaseType()).andReturn(new DatabaseType("postgres")).atLeastOnce();
        expect(mockContext.getConnection()).andThrow(new SQLException("An exception during getConnection"));
        contextControl.replay();

        try
        {
            table = new PatchTable(mockContext);
            fail("Expected a MigrationException");
        } 
        catch (MigrationException e) 
        {
            contextControl.verify();
        }
    }

    /**
     * Validates that the system recognizes an existing patches table.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testVerifyPatchesTable() throws Exception
    {
        // Test-specific setup
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        handler.prepareGlobalResultSet(rs);
        rs.addRow(new Integer[] {new Integer(13)});

        table = new PatchTable(context);

        commonVerifications();
        verifyNotCommitted();
        verifyPreparedStatementParameter(0, 1, "milestone");
        verifyPreparedStatementNotPresent(context.getSql(DatabaseType.CREATE_PATCH_TABLE_STATEMENT_KEY));
    }
    
    /**
     * Validates that <code>getPatchLevel</code> works on an existing system.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testGetPatchLevel() throws Exception
    {
        // Test-specific setup
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        rs.addRow(new Integer[]{new Integer(13)});
        handler.prepareGlobalResultSet(rs);

        table = new PatchTable(context);
        int i = table.getPatchLevel();

        assertEquals(13, i);
        commonVerifications();
        verifyNotCommitted();
        verifyPreparedStatementParameter(1, 1, "milestone");
        verifyPreparedStatementNotPresent(context.getSql(DatabaseType.INSERT_PATCH_STATEMENT_KEY));
    }

    /**
     * Adds a result set to a data source mock to simulate existence of a patch level record.
     *
     * @param handler prepared statement handler
     * @param levelExists controls what the mock returns for whether a level was already created or not
     */
    private void setupLevelCreatedMock(PreparedStatementResultSetHandler handler, Boolean levelExists) throws MigrationException{
        MockResultSet rs = handler.createResultSet();
        handler.prepareResultSet(context.getSql(DatabaseType.PATCH_COUNT_STATEMENT_KEY), rs);
        rs.addRow(new Integer[] {BooleanUtils.toInteger(levelExists)});
        table = new PatchTable(context);
    }

    /**
     * Validates that <code>getPatchLevel</code> works on a new system.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testGetPatchLevelFirstTime() throws Exception
    {
        // Test-specific setup
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        // empty result set
        handler.prepareResultSet(context.getSql(DatabaseType.CURRENT_PATCH_LEVEL_STATEMENT_KEY), rs);
        handler.prepareThrowsSQLException(context.getSql(DatabaseType.PATCH_TABLE_EXISTS_STATEMENT_KEY));
        setupLevelCreatedMock(handler, false);

        int i = table.getPatchLevel();

        assertEquals(0, i);
        commonVerifications();
        verifyPreparedStatementPresent(context.getSql(DatabaseType.INSERT_PATCH_STATEMENT_KEY));
    }

    /**
     * Validates that the patch level can be updated.
     *  
     * @throws Exception if an unexpected error occurs
     */
    public void testUpdatePatchLevel() throws Exception
    {
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        rs.addRow(new Integer[]{new Integer(12)});
        handler.prepareResultSet(context.getSql(DatabaseType.CURRENT_PATCH_LEVEL_STATEMENT_KEY), rs, new String[]{"milestone"});
        setupLevelCreatedMock(handler, true);

        table.updatePatchLevel(13);
        
        verifyPreparedStatementParameter(context.getSql(DatabaseType.UPDATE_PATCH_STATEMENT_KEY), 1, new Integer(13));
        verifyPreparedStatementParameter(context.getSql(DatabaseType.UPDATE_PATCH_STATEMENT_KEY), 2, "milestone");
        commonVerifications();
        verifyCommitted();
    }
    
    /**
     * Validates that <code>isPatchTableLocked</code> works when no lock exists.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testIsPatchTableNotLocked() throws Exception
    {
        // Test-specific setup
        // Return a non-empty set in response to the patch lock query
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        rs.addRow(new String[]{"F"});
        handler.prepareResultSet(context.getSql(DatabaseType.LOCK_STATUS_STATEMENT_KEY), rs, new String[]{"milestone", "milestone"});
        setupLevelCreatedMock(handler, true);
        
        assertFalse(table.isPatchStoreLocked());
        commonVerifications();
        verifyNotCommitted();
    }
    
    /**
     * Validates that <code>isPatchTableLocked</code> works when a lock already exists.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testIsPatchTableLocked() throws Exception
    {
        // Test-specific setup
        // Return a non-empty set in response to the patch lock query
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        rs.addRow(new String[]{"T"});
        handler.prepareResultSet(context.getSql(DatabaseType.LOCK_STATUS_STATEMENT_KEY), rs, new String[]{"milestone", "milestone"});
        setupLevelCreatedMock(handler, true);
        
        assertTrue(table.isPatchStoreLocked());
        commonVerifications();
        verifyNotCommitted();
    }
    
    /**
     * Validates that an <code>IllegalStateException</code> is thrown when trying
     * to lock an already locked patches table.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testLockPatchTableWhenAlreadyLocked() throws Exception
    {
        // Test-specific setup
        // Return a non-empty set in response to the patch lock query
        handler = conn.getPreparedStatementResultSetHandler();
        handler.prepareUpdateCount(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 0, new String[] {"milestone", "milestone"});
        setupLevelCreatedMock(handler, true);
        
        try
        {
            table.lockPatchStore();
            fail("Expected an IllegalStateException since a lock already exists.");
        }
        catch (IllegalStateException e)
        {
            // Expected
        }
        
        verifyPreparedStatementParameter(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 1, "milestone");
        verifyPreparedStatementParameter(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 2, "milestone");
        commonVerifications();
        verifyCommitted();
    }

    /**
     * Validates that the patches table can be locked as long as no other lock
     * is in place.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testLockPatchTableWhenNotAlreadyLocked() throws Exception
    {
        // Test-specific setup
        // Return an empty set in response to the patch lock query
        handler = conn.getPreparedStatementResultSetHandler();
        setupLevelCreatedMock(handler, true);
        handler.prepareUpdateCount(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 1, new String[] {"milestone", "milestone"});
        
        table.lockPatchStore();
        verifyPreparedStatementParameter(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 1, "milestone");
        verifyPreparedStatementParameter(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 2, "milestone");
        commonVerifications();
        verifyCommitted();
    }
    
    /**
     * Validates that the patches table lock can be removed.
     * 
     * @throws Exception if an unexpected error occurs
     */
    public void testUnlockPatchTable() throws Exception
    {
        // Return a non-empty set in response to the patch lock query
        handler = conn.getPreparedStatementResultSetHandler();
        handler.prepareUpdateCount(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 0, new String[] {"milestone", "milestone"});
        setupLevelCreatedMock(handler, true);
        table.unlockPatchStore();

        verifyPreparedStatementParameter(context.getSql(DatabaseType.LOCK_RELEASE_STATEMENT_KEY), 1, "milestone");
        commonVerifications();
        verifyCommitted();
    }

    public void testIsPatchApplied() throws MigrationException
    {
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        rs.addRow(new Integer[]{new Integer(3)});
        handler.prepareGlobalResultSet(rs);
        table = new PatchTable(context);

        assertEquals(true, table.isPatchApplied(3));
        commonVerifications();
        verifyPreparedStatementPresent(context.getSql(DatabaseType.PATCH_EXISTS_STATEMENT_KEY));

    }

    public void testMigrationExceptionIsThrownIfSQLExceptionHappens() throws SQLException {

        expect(mockContext.getDatabaseType()).andReturn(new DatabaseType("postgres")).atLeastOnce();
        expect(mockContext.getConnection()).andThrow(new SQLException("An exception during getConnection"));
        contextControl.replay();

        try {

            table = new PatchTable(mockContext);
            table.isPatchApplied(3);
            fail("MigrationException should have happened if SQLException");

        } catch (MigrationException e) {

            //Expected
        }

    }

    public void testIsPatchAppliedWithMissingLevel() throws MigrationException
    {
        // Return a non-empty set in response to the patch lock query
        handler = conn.getPreparedStatementResultSetHandler();
        handler.prepareUpdateCount(context.getSql(DatabaseType.LOCK_OBTAIN_STATEMENT_KEY), 0, new String[] {"milestone", "milestone"});
        MockResultSet rs = handler.createResultSet();
        handler.prepareResultSet(context.getSql(DatabaseType.PATCH_EXISTS_STATEMENT_KEY), rs);
        setupLevelCreatedMock(handler, true);
        table = new PatchTable(context);

        assertEquals(false, table.isPatchApplied(3));
        commonVerifications();
        verifyPreparedStatementPresent(context.getSql(DatabaseType.PATCH_EXISTS_STATEMENT_KEY));
    }

    public void testPatchRetrievesSetWithPatchesApplied () throws SQLException, MigrationException {
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        rs.addColumn("patch_level", new Object[]{1, 2});
        handler.prepareGlobalResultSet(rs);

        Set<Integer> expected = new HashSet<Integer>();
        expected.add(1);
        expected.add(2);
        table = new PatchTable(context);
        assertEquals(expected, table.getPatchesApplied());
        commonVerifications();
        verifyPreparedStatementPresent(context.getSql(DatabaseType.GET_ALL_PATCHES_STATEMENT_KEY));
    }

    public void testSecondSystemInitialized () throws Exception, SQLException, MigrationException {
        String systemName = "milestone";
        // setup test like testGetPatchLevelFirstTime
        handler = conn.getPreparedStatementResultSetHandler();
        MockResultSet rs = handler.createResultSet();
        // empty result set
        handler.prepareResultSet(context.getSql(DatabaseType.CURRENT_PATCH_LEVEL_STATEMENT_KEY), rs, Arrays.asList(systemName));
        handler.prepareThrowsSQLException(context.getSql(DatabaseType.PATCH_TABLE_EXISTS_STATEMENT_KEY));
        setupLevelCreatedMock(handler, false);

        int i = table.getPatchLevel();

        assertEquals(0, i);
        commonVerifications();
        verifyPreparedStatementPresent(context.getSql(DatabaseType.INSERT_PATCH_STATEMENT_KEY));
        verifyPreparedStatementParameter(context.getSql(DatabaseType.INSERT_PATCH_STATEMENT_KEY), 1, systemName);

        // Test-specific setup
        // setup a second system backed by the same database table and verify that it is initialized properly
        systemName = "orders";
        DataSourceMigrationContext context2 = new DataSourceMigrationContext();
        context2.setDataSource(new ConnectionWrapperDataSource(conn));
        context2.setSystemName(systemName);
        context2.setDatabaseType(new DatabaseType("hsqldb"));

        // clear the exception from above which caused the database table to be created
        handler.clearThrowsSQLException();
        // clear out prepared statements since one is for "patches.create"
        handler.clearPreparedStatements();
        // simulate the table existing (since it was created by the first system)
        rs = handler.createResultSet();
        handler.prepareResultSet(context.getSql(DatabaseType.PATCH_TABLE_EXISTS_STATEMENT_KEY), rs, Arrays.asList(systemName));
        rs = handler.createResultSet();
        handler.prepareResultSet(context.getSql(DatabaseType.CURRENT_PATCH_LEVEL_STATEMENT_KEY), rs, Arrays.asList(systemName));
        PatchTable table2 = new PatchTable(context2);

        i = table2.getPatchLevel();
        assertEquals(0, i);
        verifyPreparedStatementNotPresent(context.getSql(DatabaseType.CREATE_PATCH_TABLE_STATEMENT_KEY));
        verifyPreparedStatementPresent(context.getSql(DatabaseType.INSERT_PATCH_STATEMENT_KEY));
        verifyPreparedStatementParameter(context.getSql(DatabaseType.INSERT_PATCH_STATEMENT_KEY), 1, systemName);
    }

    private void commonVerifications()
    {
        verifyAllResultSetsClosed();
        verifyAllStatementsClosed();
        verifyConnectionClosed();
    }


}
