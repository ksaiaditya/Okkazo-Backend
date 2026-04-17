package com.okkazo.authservice.dtos;

import java.time.LocalDateTime;
import java.util.UUID;

public record UserPasswordChangedEvent(
        String type,
        UUID authId,
        String email,
        String username,
        LocalDateTime changedAt
) {
}
