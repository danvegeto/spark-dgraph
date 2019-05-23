import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Person implements DGraphVertex, Serializable {

    public Person(String uid, String firstName, String lastName, String company, String title, String city) {
        this.uid = uid;
        this.firstName = firstName;
        this.lastName = lastName;
        this.company = company;
        this.title = title;
        this.city = city;

        friends = new ArrayList<>();
    }

    String uid;
    String firstName;
    String lastName;
    String company;
    String title;
    String city;
    List<Person> friends; // corresponds to 'friends' edge in DGraph schema

    @Override
    public String getUid() {
        return uid;
    }

    @Override
    public List<Person> getEdges(String edgeType) {
        if(edgeType.equals("friends"))
            return friends;
        else
            return null;
    }
}