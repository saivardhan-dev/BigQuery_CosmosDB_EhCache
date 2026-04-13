package com.dataplatform.bq_cdb_ch.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

/**
 * Generic paginated API response wrapper.
 *
 * @param <T> the type of items in this page
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PagedResponse<T> {

    private List<T> content;

    private int page;
    private int size;
    private long totalElements;
    private int totalPages;

    private boolean first;
    private boolean last;
    private boolean hasNext;
    private boolean hasPrevious;

    /**
     * Source of data: "cache" or "database" — useful for observability.
     */
    private String dataSource;

    public static <T> PagedResponse<T> of(
            List<T> content,
            int page,
            int size,
            long totalElements,
            String dataSource) {

        int totalPages = size == 0 ? 1 : (int) Math.ceil((double) totalElements / size);

        return PagedResponse.<T>builder()
                .content(content)
                .page(page)
                .size(size)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .first(page == 0)
                .last(page >= totalPages - 1)
                .hasNext(page < totalPages - 1)
                .hasPrevious(page > 0)
                .dataSource(dataSource)
                .build();
    }
}