package edu.miu.mapreduce;

public class Test {

    public static void main(String[] args) {

        String testSplit = "64.242.88.10 - - [07/Mar/2004:16:11:58 -0800] \"GET /twiki/bin/view/TWiki/WikiSyntax HTTP/1.1\" 200 7352";
        String[] arrOfStr = testSplit.split("\\s+");
        System.out.println("IP Address: " + arrOfStr[0] + " Quantity: " + arrOfStr[9]);
    }
}
