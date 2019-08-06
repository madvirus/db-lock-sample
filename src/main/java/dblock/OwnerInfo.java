package dblock;

import java.time.LocalDateTime;

public class OwnerInfo {
    public final String owner;
    public final LocalDateTime expiry;

    public OwnerInfo(String owner, LocalDateTime expiry) {
        this.owner = owner;
        this.expiry = expiry;
    }
}
