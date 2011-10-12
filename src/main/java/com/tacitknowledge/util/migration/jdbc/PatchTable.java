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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tacitknowledge.util.migration.MigrationException;
import com.tacitknowledge.util.migration.MigrationTask;
import com.tacitknowledge.util.migration.PatchInfoStore;
import com.tacitknowledge.util.migration.jdbc.util.SqlUtil;


/**
 * Manages interactions with the "patches" table.  The patches table stores
 * the current patch level for a given system, as well as a system-scoped lock
 * use to avoid concurrent patches to the system.  A system is defined as an
 * exclusive target of a patch.
 * <p>
 * This class is responsible for:
 * <ul>
 *    <li>Validating the existence of the patches table and creating it if it
 *        doesn't exist</li>
 *    <li>Determining if a patch is currently running on a given system</li>
 *    <li>Obtaining and releasing patch locks for a given system</li>
 *    <li>Obtaining and incrementing the patch level for a given system</li>
 * </ul>
 * <p>
 * <strong>TRANSACTIONS:</strong> Transactions should be committed by the calling
 * class as needed.  This class does not explictly commit or rollback transactions.
 * 
 * @author  Scott Askew (scott@tacitknowledge.com)
 */
public class PatchTable implements PatchInfoStore
{
    /** Class logger */
    private static Log log = LogFactory.getLog(PatchTable.class);
    
    /** The migration configuration */
    private JdbcMigrationContext context = null;
    
    /** Keeps track of table validation (see #createPatchesTableIfNeeded) */
    private boolean tableExistenceValidated = false;

    /** Keeps track of the patch_runs table existence */
    private boolean runsTableExistenceValidated = false;
    
    /**
     * Create a new <code>PatchTable</code>.
     * 
     * @param migrationContext the migration configuration and connection source
     */
    public PatchTable(JdbcMigrationContext migrationContext)
    {
        this.context = migrationContext;
        
        if (context.getDatabaseType() == null)
        {
            throw new IllegalArgumentException("The JDBC database type is required");
        }
    }

    /** {@inheritDoc} */
    public void createPatchStoreIfNeeded() throws MigrationException
    {
        if (tableExistenceValidated && runsTableExistenceValidated)
        {
            return;
        }
        
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try
        {
            conn = context.getConnection();

            stmt = conn.prepareStatement(getSql("level.read"));
            stmt.setString(1, context.getSystemName());
            rs = stmt.executeQuery();
            log.debug("'patches' table already exists.");
            tableExistenceValidated = true;
        }
        catch (SQLException e)
        {
            SqlUtil.close(null, stmt, rs);
            createTrackingTable("patches", conn, "patches.create", e);
            tableExistenceValidated = true;
        }
        catch (Exception ex)
        {
            throw new MigrationException("Unexpected exception while creating patch store.", ex);
        }
        finally
        {
            SqlUtil.close(conn, stmt, rs);
        }

        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("run.check"));
            rs = stmt.executeQuery();
            log.debug("'patches_run' table already exists.");
            runsTableExistenceValidated = true;
        }
        catch (SQLException e)
        {
            SqlUtil.close(null, stmt, rs);
            createTrackingTable("patch_runs", conn, "patch_runs.create", e);
            runsTableExistenceValidated = true;
        }
        catch (Exception ex)
        {
            throw new MigrationException("Unexpected exception while creating patch store.", ex);
        }
        finally
        {
            SqlUtil.close(conn, stmt, rs);
        }
    }
    
    /** {@inheritDoc} */
    public int getPatchLevel() throws MigrationException
    {
        createPatchStoreIfNeeded();

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("level.read"));
            stmt.setString(1, context.getSystemName());
            rs = stmt.executeQuery();
            if (rs.next())
            {
                return rs.getInt(1);
            }

            SqlUtil.close(conn, stmt, rs);
            conn = null;
            stmt = null;
            rs = null;
            
            // We don't yet have a patch record for this system; create one
            createSystemPatchRecord();
            return 0;
        }
        catch (SQLException e)
        {
            throw new MigrationException("Unable to get patch level", e);
        }
        finally
        {
            SqlUtil.close(conn, stmt, rs);
        }
    }
    
    /** {@inheritDoc} */
    public void updatePatchLevel(int level) throws MigrationException
    {
        // Make sure a patch record already exists for this system
        getPatchLevel();
        
        Connection conn = null;
        PreparedStatement stmt = null;
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("level.update"));
            stmt.setInt(1, level);
            stmt.setString(2, context.getSystemName());
            stmt.execute();
            context.commit();
        }
        catch (SQLException e)
        {
            throw new MigrationException("Unable to update patch level", e);
        }
        finally
        {
            SqlUtil.close(conn, stmt, null);
        }
    }
    
    /** {@inheritDoc} */
    public boolean isPatchStoreLocked() throws MigrationException
    {
        createPatchStoreIfNeeded();
        
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("lock.read"));
            stmt.setString(1, context.getSystemName());
            rs = stmt.executeQuery();
            
            if (rs.next())
            {
                return ("T".equals(rs.getString(1)));
            }
            else
            {
                return false;
            }
        }
        catch (SQLException e)
        {
            throw new MigrationException("Unable to determine if table is locked", e);
        }
        finally
        {
            SqlUtil.close(conn, stmt, rs);
        }
    }
    
    /** {@inheritDoc} */
    public void lockPatchStore() throws MigrationException, IllegalStateException
    {
        if (isPatchStoreLocked())
        {
            throw new IllegalStateException("Patch table is already locked!");
        }
        updatePatchLock(true);
    }

    /** {@inheritDoc} */
    public void unlockPatchStore() throws MigrationException
    {
        updatePatchLock(false);
    }

    /** {@inheritDoc} */
    public void recordPatchStart(MigrationTask task) throws MigrationException
    {
        createPatchStoreIfNeeded();

        Connection conn = null;
        PreparedStatement stmt = null;
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("run.create"));
            stmt.setString(1, context.getSystemName());
            stmt.setString(2, context.getDatabaseName());
            stmt.setInt(3, task.getLevel().intValue());
            stmt.setString(4, task.getName());
            stmt.execute();
            context.commit();
        }
        catch (SQLException e)
        {
            throw new MigrationException("Unable to record patch start", e);
        }
        finally
        {
            SqlUtil.close(conn, stmt, null);
        }
    }

    /** {@inheritDoc} */
    public void recordPatchStop(MigrationTask task) throws MigrationException
    {
        createPatchStoreIfNeeded();

        Connection conn = null;
        PreparedStatement stmt = null;
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("run.update"));
            stmt.setString(1, context.getSystemName());
            stmt.setString(2, context.getDatabaseName());
            stmt.setInt(3, task.getLevel().intValue());
            stmt.execute();
            context.commit();
        }
        catch (SQLException e)
        {
            throw new MigrationException("Unable to record patch stop", e);
        }
        finally
        {
            SqlUtil.close(conn, stmt, null);
        }
    }

    /**
     * Returns the SQL to execute for the database type associated with this patch table.
     * 
     * @param  key the key within <code><i>database</i>.properties</code> whose
     *         SQL should be returned
     * @return the SQL to execute for the database type associated with this patch table
     */
    protected String getSql(String key)
    {
        return context.getDatabaseType().getProperty(key);
    }
    
    /**
     * Creates an initial record in the patches table for this system. 
     * 
     * @exception SQLException if an unrecoverable database error occurs
     * @exception MigrationException if an unrecoverable database error occurs
     */
    private void createSystemPatchRecord() throws MigrationException, SQLException
    {
        String systemName = context.getSystemName();
        Connection conn = null;
        PreparedStatement stmt = null;
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql("level.create"));
            stmt.setString(1, systemName);
            stmt.execute();
            context.commit();
            log.info("Created patch record for " + systemName);
        }
        catch (SQLException e)
        {
            log.error("Error creating patch record for system '" + systemName + "'", e);
            throw e;
        }
        finally
        {
            SqlUtil.close(conn, stmt, null);
        }
    }

    /**
     * Obtains or releases a lock for this system in the patches table. 
     * 
     * @param  lock <code>true</code> if a lock is to be obtained, <code>false</code>
     *         if it is to be removed 
     * @throws MigrationException if an unrecoverable database error occurs
     */        
    private void updatePatchLock(boolean lock) throws MigrationException
    {
        String sqlkey = (lock) ? "lock.obtain" : "lock.release";
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try
        {
            conn = context.getConnection();
            stmt = conn.prepareStatement(getSql(sqlkey));
            if (log.isDebugEnabled())
            {
                log.debug("Updating patch table lock: " + getSql(sqlkey));
            }
            stmt.setString(1, context.getSystemName());
            stmt.execute();
            context.commit();
        }
        catch (SQLException e)
        {
            throw new MigrationException("Unable to update patch lock to " + lock, e);
        }
        finally
        {
            SqlUtil.close(conn, stmt, null);
        }
    }

    /**
     * Create the tables used to track patch information. Merely a means to avoid duplicating code.
     *
     * @param tableName a human-readable name for the table for logging purposes
     * @param conn the Connection to use for the table creation
     * @param sqlKey the key into the DatabaseType for the DDL to use to create the table
     * @param triggerException the exception that caused this table creation for logging purposes
     * @throws MigrationException if an unrecoverable database error occurs
     */
    private void createTrackingTable(String tableName, Connection conn, String sqlKey, SQLException triggerException)
        throws MigrationException
    {
        // logging error in case it's not a simple patch table doesn't exist error
        log.debug(triggerException.getMessage());

        // check connection is valid before using, because the getConnection() call
        // could have thrown the SQLException
        if (null == conn)
        {
            throw new MigrationException("Unable to create a connection.", triggerException);
        }

        log.info("'" + tableName + "' table must not exist; creating....");
        PreparedStatement stmt = null;
        try
        {
            stmt = conn.prepareStatement(getSql(sqlKey));
            if (log.isDebugEnabled())
            {
                log.debug("Creating " + tableName + " table with SQL '" + getSql(sqlKey) + "'");
            }
            stmt.execute();
            context.commit();
            log.info("Created '" + tableName + "' table.");
        }
        catch (SQLException sqle)
        {
            throw new MigrationException("Unable to create " + tableName + " table", sqle);
        }
        finally
        {
            SqlUtil.close(null, stmt, null);
        }
    }
}
