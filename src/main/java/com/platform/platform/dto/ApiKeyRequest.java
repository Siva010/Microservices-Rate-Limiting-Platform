package com.platform.platform.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class ApiKeyRequest {
    @NotBlank
    private String name;
    private String key;
}