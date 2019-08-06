package dblock;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

public class DbLock {
    private final String ownerId;
    private final DataSource dataSource;

    public DbLock(DataSource dataSource) {
        this.dataSource = dataSource;
        this.ownerId = UUID.randomUUID().toString();
    }

    public void runInLock(String name, Duration duration, Runnable runnable) {
        if (getLock(name, duration)) {
            runnable.run();
        }
    }

    private boolean getLock(String name, Duration duration) {
        Connection conn = null;
        boolean owned;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            OwnerInfo ownerInfo = getLockOwner(conn, name);
            if (ownerInfo == null) {
                insertLockOwner(conn, name, duration);
                owned = true;
            } else if (ownerInfo.owner.equals(this.ownerId)) {
                updateLockOwner(conn, name, duration);
                owned = true;
            } else if (ownerInfo.expiry.isBefore(LocalDateTime.now())) {
                updateLockOwner(conn, name, duration);
                owned = true;
            } else {
                owned = false;
            }
            conn.commit();
        } catch (Exception e) {
            owned = false;
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                }
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(false);
                } catch(SQLException ex) {}
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        return owned;
    }

    private OwnerInfo getLockOwner(Connection conn, String name) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("select * from dist_lock where name = ? for update")) {
            pstmt.setString(1, name);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new OwnerInfo(
                            rs.getString("owner"),
                            rs.getTimestamp("expiry").toLocalDateTime());
                }
            }
        }
        return null;
    }

    private void insertLockOwner(Connection conn, String name, Duration duration) throws SQLException {
        try(PreparedStatement pstmt = conn.prepareStatement("insert into dist_lock values (?, ?, ?)")) {
            pstmt.setString(1, name);
            pstmt.setString(2, ownerId);
            pstmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now().plusSeconds(duration.getSeconds())));
            pstmt.executeUpdate();
        }
    }

    private void updateLockOwner(Connection conn, String name, Duration duration) throws SQLException {
        try(PreparedStatement pstmt = conn.prepareStatement("update dist_lock set owner = ?, expiry = ? where name = ?")) {
            pstmt.setString(1, ownerId);
            pstmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now().plusSeconds(duration.getSeconds())));
            pstmt.setString(3, name);
            pstmt.executeUpdate();
        }

    }
}
