package org.hdf5javalib.utils;

public class HdfTypeUtils {
//    /**
//     * Instantiates and populates a BigInteger or BigDecimal field of a class with a fixed-point value.
//     *
//     * @param clazz Class type to create and populate
//     * @param fieldName name of field in class to populate
//     * @param hdfFixedPoint fixed-point value.
//     * @param scale scale to set of the field of the class is a BigDecimal
//     * @return Class instance with field populated
//     * @param <T> Class type
//     */
//    @SneakyThrows
//    public static <T> T populateFromFixedPoint(Class<T> clazz, String fieldName, HdfFixedPoint hdfFixedPoint, int scale) {
//        Field field = getField(clazz.getDeclaredFields(), fieldName, clazz.getName());
//        return populateFromFixedPoint(clazz, field, hdfFixedPoint, scale);
//    }
//
//    /**
//     * Instantiates and populates a BigInteger or BigDecimal field of a class with a fixed-point value.
//     *
//     * @param clazz Class type to create and populate
//     * @param field Field in class to populate
//     * @param hdfFixedPoint fixed-point value.
//     * @param scale scale to set of the field of the class is a BigDecimal
//     * @return Class instance with field populated
//     * @param <T> Class type
//     */
//    @SneakyThrows
//    public static <T> T populateFromFixedPoint(Class<T> clazz, Field field, HdfFixedPoint<BigInteger> hdfFixedPoint, int scale) {
//        T instance = clazz.getDeclaredConstructor().newInstance();
//        Class<?> fieldType = field.getType();
//        if (fieldType != BigInteger.class && fieldType != BigDecimal.class) {
//            throw new IllegalArgumentException("Field " + field.getName() + " must be BigInteger/BigDecimal.");
//        }
//        if (fieldType == BigInteger.class) {
//            BigInteger value = hdfFixedPoint.getInstance();
//            field.set(instance, value);
//        } else {
//            BigDecimal value = hdfFixedPoint.toBigDecimal(scale);
//            field.set(instance, value);
//        }
//        return instance;
//    }
//
//    @SneakyThrows
//    public static <T> T populateFromString(Class<T> clazz, String fieldName, HdfString hdfString) {
//        Field field = getField(clazz.getDeclaredFields(), fieldName, clazz.getName());
//        return populateFromString(clazz, field, hdfString);
//    }
//
//    @SneakyThrows
//    public static <T> T populateFromString(Class<T> clazz, Field field, HdfString hdfString) {
//        T instance = clazz.getDeclaredConstructor().newInstance();
//        Class<?> fieldType = field.getType();
//        if (fieldType != String.class ) {
//            throw new IllegalArgumentException("Field " + field.getName() + " must be String");
//        }
//        field.set(instance, hdfString.getValue());
//        return instance;
//    }
//
//    @SneakyThrows
//    public static <T> T populateFromCompound(Class<T> clazz, HdfCompound hdfCompound, int scale) {
//        Map<String, Field> nameToFieldMap = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toMap(Field::getName, f -> f));
//        return populateFromCompound(clazz, nameToFieldMap, hdfCompound, scale);
//    }
//
//    @SneakyThrows
//    public static <T> T populateFromCompound(Class<T> clazz, Map<String, Field> nameToFieldMap, HdfCompound hdfCompound, int scale) {
//        // Create an instance of T
//        T instance = clazz.getDeclaredConstructor().newInstance();
//        Map<String, HdfCompoundMember> nameToMemberMap = hdfCompound.getMembers().stream().collect(Collectors.toMap(HdfCompoundMember::getName, hdfCompoundMember -> hdfCompoundMember));
//
//        // Populate fields using the pre-parsed map
//        for (Map.Entry<String, Field> entry : nameToFieldMap.entrySet()) {
//            HdfCompoundMember member = nameToMemberMap.get(entry.getKey());
//            Field field = entry.getValue();
//
//            if (field.getType() == String.class && member.getData() instanceof HdfString) {
//                field.set(instance, ((HdfString)member.getData()).getValue());
//            } else if (field.getType() == BigInteger.class && member.getData() instanceof HdfFixedPoint) {
//                field.set(instance, ((HdfFixedPoint) member.getData()).toBigInteger());
//            } else if (field.getType() == BigDecimal.class && member.getData() instanceof HdfFixedPoint) {
//                field.set(instance, ((HdfFixedPoint) member.getData()).toBigDecimal(scale));
//            }
//            // Add more type handling as needed
//        }
//
//        return instance;
//    }
//
//    private static <T> Field getField(Field[] fields, String fieldName, String className) {
//        Field field = null;
//        for (Field f : fields) {
//            f.setAccessible(true);
//            if (f.getName().equals(fieldName)) {
//                field = f;
//                break;
//            }
//        }
//        if (field == null) {
//            throw new IllegalArgumentException("Field " + fieldName + " not found in " + className);
//        }
//        return field;
//    }
//
}
