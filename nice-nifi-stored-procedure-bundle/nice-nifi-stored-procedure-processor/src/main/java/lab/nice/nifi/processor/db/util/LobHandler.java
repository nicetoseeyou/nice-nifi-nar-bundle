package lab.nice.nifi.processor.db.util;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Auto free CLOB/NCLOB handler.
 * Support to construct CLOB/NCLOB from character stream, and free CLOB/NCLOB after commit.
 */
public class LobHandler implements AutoCloseable {
    private static final int DEFAULT_BUFFER_SIZE = 256;

    private final Statement statement;
    private final List<Clob> clobs;
    private final List<NClob> nClobs;

    /**
     * Construct handler with statement.
     *
     * @param statement the sql statement
     */
    public LobHandler(final Statement statement) {
        this.statement = statement;
        this.clobs = new ArrayList<>();
        this.nClobs = new ArrayList<>();
    }

    /**
     * Create CLOB from character stream.
     *
     * @param reader the character stream to retrieve
     * @return CLOB
     * @throws SQLException if failed to create CLOB
     * @throws IOException  if failed to read character stream
     */
    public Clob clob(final Reader reader) throws SQLException, IOException {
        if (null == reader) {
            return null;
        } else {
            final Clob clob = statement.getConnection().createClob();
            writeToClob(clob, reader);
            return clob;
        }
    }

    /**
     * Create NCLOB from character stream.
     *
     * @param reader the character stream to retrieve
     * @return NCLOB
     * @throws SQLException if failed to create NCLOB
     * @throws IOException  if failed to read character stream
     */
    public NClob nClob(final Reader reader) throws SQLException, IOException {
        if (null == reader) {
            return null;
        } else {
            final NClob nClob = statement.getConnection().createNClob();
            writeToClob(nClob, reader);
            return nClob;
        }
    }

    /**
     * Write character to CLOB from character stream.
     *
     * @param clob   the target CLOB
     * @param reader the character stream
     * @throws IOException  if failed to read character stream
     * @throws SQLException if failed to write character to CLOB
     */
    private void writeToClob(final Clob clob, final Reader reader) throws IOException, SQLException {
        final CharBuffer charBuffer = CharBuffer.allocate(DEFAULT_BUFFER_SIZE);
        long position = 1;
        while (reader.read(charBuffer) != -1) {
            charBuffer.flip();
            clob.setString(position, charBuffer.toString());
            position = position + charBuffer.limit();
            charBuffer.clear();
        }
    }

    /**
     * Auto close (free) CLOB/NCLOB
     *
     * @throws SQLException if failed to free CLOB/NCLOB
     * @see AutoCloseable#close()
     */
    @Override
    public void close() throws SQLException {
        for (Clob clob : clobs) {
            clob.free();
        }
        for (NClob nClob : nClobs) {
            nClob.free();
        }
    }
}
