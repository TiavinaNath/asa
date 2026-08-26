package gen.patrimoine.cas.file.hash;

import gen.patrimoine.cas.PojaGenerated;

@PojaGenerated
public record FileHash(FileHashAlgorithm algorithm, String value) {}
