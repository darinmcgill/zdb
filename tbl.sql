create table tbl_zoom (
    recordId,         -- global row uuid
    nodeId,           -- uuid
    key,              -- null,text or integer
    valType INTEGER,  --
    value,
    timeStamp REAL,   -- seconds since unix epoch
    source            -- user/program id
);
