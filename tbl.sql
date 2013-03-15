create table if not exists tbl (
    recordId,         -- global row uuid
    nodeId,           -- uuid
    key,              -- null,text or integer
    valType INTEGER,  --
    valData,
    timeStamp REAL,   -- seconds since unix epoch
    src            -- user/program id
);
