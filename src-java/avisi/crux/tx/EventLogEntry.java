package avisi.crux.tx;

import net.java.ao.RawEntity;
import net.java.ao.schema.*;

@Table("EventLogEntry")
public interface EventLogEntry extends RawEntity<Long> {

    String ID = "ID";

    @AutoIncrement
    @NotNull
    @PrimaryKey(ID)
    long getID();

    @NotNull
    @StringLength(20)
    String getTopic();

    @NotNull
    long getTime();

    @NotNull
    @StringLength(StringLength.UNLIMITED)
    String getBody();
    void setBody(String body);

    @Indexed
    @StringLength(100)
    String getKey();
}

