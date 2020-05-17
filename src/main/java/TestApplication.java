import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestApplication {

    public static void main(String[] args) {
        String[][] data = new String[][]{{"a", "b"}, {"c", "d"}, {"e", "f"}};

        Stream<String[]> stream = Arrays.stream(data);
       List<String> list= stream.flatMap(x-> Arrays.stream(x)).collect(Collectors.toList());

       System.out.println(list);
    }
}
