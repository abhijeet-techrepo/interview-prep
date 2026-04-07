package test.random;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HashMapSortByValue {

    public static void main(String[] args) {
        List<String> words = Arrays.asList("vish", "apple", "banana", "apple", "cherry", "banana", "apple");
        System.out.println("--------Approach1 ------\n"); approach1(words);
        System.out.println("--------Approach1A ------\n"); approach1A(words);
        System.out.println("--------Approach1B ------\n"); approach1B(words);


    }

    private static void approach1(List<String> words){
        // Step 1: Count frequencies
        Map<String, Integer> freqMap = new HashMap<>();
        for (String word : words) {
            freqMap.put(word, freqMap.getOrDefault(word, 0) + 1);
        }

        // Step 2: Sort by value
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(freqMap.entrySet());
        entries.sort((a, b) -> b.getValue() - a.getValue()); // descending

        for (Map.Entry<String, Integer> e : entries) {
            System.out.println(e.getKey() + " -> " + e.getValue());
        }
    }

    private static void approach1A(List<String> words){
        words.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(e -> System.out.println(e.getKey() + " -> " + e.getValue()));

    }

    private static void approach1B(List<String> words){
        words.stream()
                .collect(Collectors.toMap(  w -> w,        // key mapper
                                w -> 1L,       // value mapper (each word starts with count 1)
                                Long::sum      // merge function (add counts for duplicates))
                ))
                .entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getValue))
                .forEach(e -> System.out.println(e.getKey() + " -> " + e.getValue()));


    }

    private static
}
