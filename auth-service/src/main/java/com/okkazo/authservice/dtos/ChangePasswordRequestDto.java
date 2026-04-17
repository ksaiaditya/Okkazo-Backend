package com.okkazo.authservice.dtos;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import static com.okkazo.authservice.utils.PasswordPolicy.MESSAGE;
import static com.okkazo.authservice.utils.PasswordPolicy.PASSWORD_REGEX;

public record ChangePasswordRequestDto(
        @NotBlank(message = "Current password is required")
        String currentPassword,

        @NotBlank(message = "New password is required")
        @Size(min = 8, max = 72, message = "Password must have at least 8 characters")
        @Pattern(regexp = PASSWORD_REGEX, message = MESSAGE)
        String newPassword,

        @NotBlank(message = "Confirm password is required")
        String confirmPassword
) {
}
