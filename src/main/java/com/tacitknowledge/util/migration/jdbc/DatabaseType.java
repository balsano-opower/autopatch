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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

/**
 * Defines a type of database (e.g. <code>oracle</code> or <code>postgres</code>.  This
 * is used to help define the SQL that is used to update the patch table and as a hint
 * used while parsing SQL patch files.
 * <p/>
 * Each type of database that this AutoPatch supports (currently PostgreSQL and Oracle) has its
 * own <i>database-type</i>.properties file containing the database-specific SQL and DDL.  The
 * required keys are:
 * <ul>
 * <li>patches.create - DDL that creates the patches table</li>
 * <li>level.create - SQL that inserts a new patch level record for the system</li>
 * <li>level.count - SQL that counts patches for the system</li>
 * <li>level.read - SQL that selects the current patch level of the system</li>
 * <li>level.update - SQL that updates the current patch level of the system</li>
 * <li>lock.read - Returns 'T' if the system patch lock is in use, 'F' otherwise</li>
 * <li>lock.obtain - SQL that selects the patch lock for the system</li>
 * <li>lock.release - SQL that releases the patch lock for the system</li>
 * </ul>
 * <p/>
 * Use <i>postgres.properties</i> or <i>oracle.properties</i> as a baseline for adding
 * additional database types.
 * <p/>
 * You can override the values in the the database.properties file by placing properties in
 * your migration.properties.  The property names should remain the same and they should be
 * prepended with the database type and a period.
 * <p/>
 * For example, the property, <code>supportsMultipleStatements</code> would be overridden
 * for mysql using the property name <code>mysql.supportsMultipleStatements</code>.
 *
 * @author Scott Askew (scott@tacitknowledge.com)
 */
public class DatabaseType
{
    /**
     * The SQL statements and properties that are unique to this database flavor.
     */
    private Properties databaseProperties;

    /**
     * The migration.properties that may override databaseProperties
     */
    private Properties migrationProperties;

    /**
     * The database type
     */
    private String databaseType = "";


    /**
     * Abstract property names into constants
     */
    public static final String SUPPORTS_MULTIPLE_STATEMENTS_KEY = "supportsMultipleStatements";
    public static final String CREATE_PATCH_TABLE_STATEMENT_KEY = "patches.create";
    public static final String INSERT_PATCH_STATEMENT_KEY = "level.create";
    public static final String PATCH_COUNT_STATEMENT_KEY = "level.count";
    public static final String PATCH_TABLE_EXISTS_STATEMENT_KEY = "level.table.exists";
    public static final String CURRENT_PATCH_LEVEL_STATEMENT_KEY = "level.read";
    public static final String ROLLBACK_PATCH_STATEMENT_KEY = "level.rollback";
    public static final String UPDATE_PATCH_STATEMENT_KEY = "level.update";
    public static final String PATCH_EXISTS_STATEMENT_KEY = "level.exists";
    public static final String GET_ALL_PATCHES_STATEMENT_KEY = "patches.all";
    public static final String LOCK_STATUS_STATEMENT_KEY = "lock.read";
    public static final String LOCK_OBTAIN_STATEMENT_KEY = "lock.obtain";
    public static final String LOCK_RELEASE_STATEMENT_KEY = "lock.release";
    private static final String[] PARAMETER_CONSTANTS = {
        SUPPORTS_MULTIPLE_STATEMENTS_KEY, CREATE_PATCH_TABLE_STATEMENT_KEY, INSERT_PATCH_STATEMENT_KEY,
        PATCH_COUNT_STATEMENT_KEY, PATCH_TABLE_EXISTS_STATEMENT_KEY, CURRENT_PATCH_LEVEL_STATEMENT_KEY,
        ROLLBACK_PATCH_STATEMENT_KEY, UPDATE_PATCH_STATEMENT_KEY, PATCH_EXISTS_STATEMENT_KEY,
        GET_ALL_PATCHES_STATEMENT_KEY, LOCK_STATUS_STATEMENT_KEY, LOCK_OBTAIN_STATEMENT_KEY,
        LOCK_RELEASE_STATEMENT_KEY
    };

    /**
     * Creates a new <code>DatabaseType</code>.
     *
     * @param databaseType the type of database
     */
    public DatabaseType(String databaseType)
    {
        // required to keep old behavior where it expects the properties file in the same location
        // as the DatabaseType class because it was loaded via Class.getResourceAsStream()
        String className = this.getClass().getName();
        int index = className.lastIndexOf(".");
        String databasePropertiesFilename = className.substring(0, index).replace(".", "/") + "/" + databaseType + ".properties";
        databaseProperties = loadProperties(databasePropertiesFilename, this.getClass().getClassLoader());
        migrationProperties = new Properties();
        try
        {
            migrationProperties = loadProperties("migration.properties", Thread.currentThread().getContextClassLoader());
        }
        catch (IllegalArgumentException iae)
        {
            // this is okay, in this class, migration.properties is only used to override SQL
        }
        this.databaseType = databaseType;

        validatePropertiesSet();
    }

    /**
     * Validates that all required properties are set and no extra properties are set.
     */
    public void validatePropertiesSet()
    {
        // copy all the parameter constants to a mutable list so we can operate on it
        List<String> constants = new ArrayList<String>(Arrays.asList(this.PARAMETER_CONSTANTS));
        // get loaded property names from the database property file
        Set<String> properties = databaseProperties.stringPropertyNames();

        // look for any properties without corresponding constants
        if (!constants.containsAll(properties)) {
            properties.removeAll(constants);
            throw new IllegalArgumentException(
                    String.format("Additional properties set in property file %s.properties: %s",
                                  databaseType,
                                  StringUtils.join(properties, " ")));
        }

        // look for any properties which were not defined for this database type
        constants.removeAll(properties);
        if (!constants.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Properties not set in property file %s.properties: ",
                                  databaseType,
                                  StringUtils.join(constants, " ")));
        }
    }

    protected Properties loadProperties(String propertiesFilename, ClassLoader loader)
    {
        Properties properties = new Properties();

        InputStream is = loader.getResourceAsStream(propertiesFilename);
        if (is == null)
        {
            is = this.getClass().getResourceAsStream(propertiesFilename);
        }

        if (is == null)
        {
            throw new IllegalArgumentException("Could not find properties "
                    + " file " + propertiesFilename + ".");
        }
        try
        {
            properties.load(is);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Could not read " + propertiesFilename);
        }
        finally
        {
            try
            {
                is.close();
            }
            catch (IOException e1)
            {
                // not important
            }
        }

        return properties;
    }

    /**
     * Return the name of the database type
     *
     * @return String containing the name of the database type
     */
    public String getDatabaseType()
    {
        return databaseType;
    }

    /**
     * Returns the named property.
     *
     * @param propertyName the property to retrieve
     * @return the requested property, or <code>null</code> if it doesn't exist
     */
    public String getProperty(String propertyName)
    {
        String value = null;

        if (migrationProperties.containsKey(databaseType + "." + propertyName))
        {
            value = migrationProperties.getProperty(databaseType + "." + propertyName);
        }
        else
        {
            value = databaseProperties.getProperty(propertyName);
        }

        return value;
    }

    /**
     * Determines if the database supports multiple SQL and DDL statements in a single
     * <code>Statement.execute</code> call.
     *
     * @return if the database supports multiple SQL and DDL statements in a single
     *         <code>Statement.execute</code> call.
     */
    public boolean isMultipleStatementsSupported()
    {
        String value = this.getProperty(SUPPORTS_MULTIPLE_STATEMENTS_KEY);
        String multiStatement = (value != null) ? value : "false";
        return Boolean.valueOf(multiStatement).booleanValue();
    }

    /**
     * Useful for debugging
     *
     * @return String containing state information
     */
    public String toString()
    {
        return "DatabaseType " + getDatabaseType();
    }
}
