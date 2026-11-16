package com.taskforge.controller;
import java.util.Scanner;

public class Division {
    public static void main(String[] args){

        Scanner sc = new Scanner(System.in);
        try{
            int a = sc.nextInt();
            int b = sc.nextInt();
            if(b == 0) throw new IllegalArgumentException("Cannot divide by Zero");

            System.out.println(a / b);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
