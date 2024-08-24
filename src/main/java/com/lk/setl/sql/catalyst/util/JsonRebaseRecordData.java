package com.lk.setl.sql.catalyst.util;

public class JsonRebaseRecordData {
    public String tz;
    public long[] switches;
    public long[] diffs;

    public String getTz() {
        return tz;
    }

    public void setTz(String tz) {
        this.tz = tz;
    }

    public long[] getSwitches() {
        return switches;
    }

    public void setSwitches(long[] switches) {
        this.switches = switches;
    }

    public long[] getDiffs() {
        return diffs;
    }

    public void setDiffs(long[] diffs) {
        this.diffs = diffs;
    }
}
