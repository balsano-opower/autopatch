package com.tacitknowledge.util.migration.jdbc;

import com.tacitknowledge.util.migration.MigrationException;
import com.tacitknowledge.util.migration.PatchInfoStore;

import java.util.Set;
import java.util.LinkedHashSet;

/**
 * Provide a test implementation of PatchInfoStore
 *
 * @author Rick Balsano (rick@opower.com)
 */
public class TestPatchTable implements PatchInfoStore {
    int patchLevel = 0;
    boolean isLocked = false;
    LinkedHashSet<Integer> patches = new LinkedHashSet<Integer>();

    public TestPatchTable(JdbcMigrationContext migrationContext) throws MigrationException
    {
    }

    @Override
    public int getPatchLevel() throws MigrationException {
        return this.patchLevel;
    }

    @Override
    public void updatePatchLevel(int level) throws MigrationException {
        this.patches.add(level);
        this.patchLevel = level;
    }

    @Override
    public boolean isPatchStoreLocked() throws MigrationException {
        return this.isLocked;
    }

    @Override
    public void lockPatchStore() throws MigrationException, IllegalStateException {
        this.isLocked = true;
    }

    @Override
    public void unlockPatchStore() throws MigrationException {
        this.isLocked = false;
    }

    @Override
    public boolean isPatchApplied(int patchLevel) throws MigrationException {
        return patchLevel > this.patchLevel;
    }

    @Override
    public void updatePatchLevelAfterRollBack(int rollbackLevel) throws MigrationException {
        for (int level = this.patchLevel; level>rollbackLevel; level--) {
            if (patches.contains(level)) {
                patches.remove(level);
            }
        }
        this.patchLevel = rollbackLevel;
    }

    @Override
    public Set<Integer> getPatchesApplied() throws MigrationException {
        return patches;
    }
}
