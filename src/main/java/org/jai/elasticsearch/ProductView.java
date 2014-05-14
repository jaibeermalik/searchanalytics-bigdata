package org.jai.elasticsearch;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "productviews", type = "productview", indexStoreType = "fs", shards = 1, replicas = 0, refreshInterval = "-1")
public class ProductView {

	@Id
	private String id;
	@Version
	private Long version;

	private Long productId;

	private int viewCount;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getVersion() {
		return version;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public int getViewCount() {
		return viewCount;
	}

	public void setViewCount(int viewCount) {
		this.viewCount = viewCount;
	}

}
