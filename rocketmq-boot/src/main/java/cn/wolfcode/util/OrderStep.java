package cn.wolfcode.util;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OrderStep {
    private long orderId;
    private String desc;

    @Override
    public String toString() {
        return "OrderStep{" +
                "orderId=" + orderId +
                ", desc='" + desc + '\'' +
                '}';
    }
}