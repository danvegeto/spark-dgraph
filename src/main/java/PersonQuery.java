import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PersonQuery implements DGraphQuery<Person>, Serializable {

    public PersonQuery() {
        all = new ArrayList<>();
    }

    List<Person> all;

    public List<Person> getAll() {
        return all;
    }
}
