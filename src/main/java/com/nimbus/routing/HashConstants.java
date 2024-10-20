package com.nimbus.routing;

import net.openhft.hashing.LongHashFunction;

public final class HashConstants {

    public static final long HASH_SEED = 0xDEADBEEF;

    public static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3(HASH_SEED);

}
