package com.okkazo.authservice.dtos;

public record ChangePasswordResponseDto(
        String message,
        boolean success
) {
}
