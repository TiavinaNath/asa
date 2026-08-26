package gen.patrimoine.cas.endpoint.event.consumer.model;

import gen.patrimoine.cas.PojaGenerated;
import gen.patrimoine.cas.endpoint.event.model.PojaEvent;

@PojaGenerated
public record TypedEvent(String typeName, PojaEvent payload) {}
