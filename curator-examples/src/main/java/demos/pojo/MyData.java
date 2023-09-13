package demos.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class MyData implements Serializable {
    private String name;
    private int age;

}
