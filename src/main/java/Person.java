import java.util.LinkedList;
import java.util.List;

public class Person {

    public Person(String uid, String firstName, String lastName, String company, String title, String city) {
        this.uid = uid;
        this.firstName = firstName;
        this.lastName = lastName;
        this.company = company;
        this.title = title;
        this.city = city;

        friends = new LinkedList<>();
    }

    String uid;
    String firstName;
    String lastName;
    String company;
    String title;
    String city;
    List<Person> friends;
}