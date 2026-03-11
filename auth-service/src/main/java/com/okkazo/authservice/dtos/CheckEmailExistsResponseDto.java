package com.okkazo.authservice.dtos;

public record CheckEmailExistsResponseDto(
        boolean exists,
        String role,
        String message
) {
}
