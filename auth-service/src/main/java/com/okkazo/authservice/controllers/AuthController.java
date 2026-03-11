package com.okkazo.authservice.controllers;

import com.okkazo.authservice.dtos.*;
import com.okkazo.authservice.models.ServiceCategory;
import com.okkazo.authservice.services.AuthService;
import com.okkazo.authservice.services.EmailVerificationService;
import com.okkazo.authservice.services.PasswordResetService;
import com.okkazo.authservice.services.RefreshTokenService;
import com.okkazo.authservice.services.VendorRegistrationService;
import com.okkazo.authservice.validators.VendorRegistrationValidator;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/")
@RequiredArgsConstructor
public class AuthController {
    private final AuthService authService;
    private final EmailVerificationService emailVerificationService;
    private final RefreshTokenService refreshTokenService;
    private final PasswordResetService passwordResetService;
    private final VendorRegistrationService vendorRegistrationService;
    private final VendorRegistrationValidator vendorRegistrationValidator;

    @PostMapping("/register")
    public ResponseEntity<RegisterResponseDto> register(@Valid @RequestBody RegisterRequestDto registerDto){
        return ResponseEntity.ok(
                authService.register(registerDto)
        );
    }

    @PostMapping("/login")
    public ResponseEntity<LoginResponseDto> login(@Valid @RequestBody LoginRequestDto loginDto){
        return ResponseEntity.ok(
                authService.login(loginDto)
        );
    }

    @PostMapping("/verify-email")
    public ResponseEntity<VerifyEmailResponseDto> verifyEmail(@RequestParam("token") String token){
        return ResponseEntity.ok(emailVerificationService.verifyEmail(token));
    }

    @PostMapping("/resend-verification")
    public ResponseEntity<ResendVerificationResponseDto> resendVerification(
            @Valid @RequestBody ResendVerificationRequestDto requestDto){
        return ResponseEntity.ok(emailVerificationService.resendVerification(requestDto));
    }

    @PostMapping("/refresh-token")
    public ResponseEntity<RefreshTokenResponseDto> refreshToken(
            @Valid @RequestBody RefreshTokenRequestDto requestDto){
        return ResponseEntity.ok(refreshTokenService.refreshAccessToken(requestDto));
    }

    @PostMapping("/forgot-password")
    public ResponseEntity<ForgotPasswordResponseDto> forgotPassword(
            @Valid @RequestBody ForgotPasswordRequestDto requestDto){
        return ResponseEntity.ok(passwordResetService.forgotPassword(requestDto));
    }

    @PostMapping("/reset-password")
    public ResponseEntity<ResetPasswordResponseDto> resetPassword(
            @Valid @RequestBody ResetPasswordRequestDto requestDto){
        return ResponseEntity.ok(passwordResetService.resetPassword(requestDto));
    }
    
    /**
     * Check if an email exists in the system
     * @param email The email address to check
     * @return CheckEmailExistsResponseDto with exists flag, role, and message
     */
    @GetMapping("/check-email")
    public ResponseEntity<CheckEmailExistsResponseDto> checkEmailExists(
            @RequestParam("email") String email){
        return ResponseEntity.ok(authService.checkEmailExists(email));
    }
    
    /**
     * Register a new vendor with business details and documents
     * 
     * Valid Service Categories:
     * - Venue
     * - Catering & Drinks
     * - Photography
     * - Videography
     * - Decor & Styling
     * - Entertainment & Artists
     * - Makeup & Grooming
     * - Invitations & Printing
     * - Sound & Lighting
     * - Equipment Rental
     * - Security & Safety
     * - Transportation
     * - Live Streaming & Media
     * - Cake & Desserts
     * - Other (requires customService parameter)
     */
    @PostMapping(value = "/vendor/register", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> registerVendor(
            @RequestParam("businessName") String businessName,
            @RequestParam("serviceCategory") String serviceCategory,
            @RequestParam(value = "customService", required = false) String customService,
            @RequestParam("email") String email,
            @RequestParam("phone") String phone,
            @RequestParam(value = "locationData", required = false) String locationDataJson,
            @RequestParam(value = "description", required = false) String description,
            @RequestParam(value = "businessLicense", required = false) MultipartFile businessLicense,
            @RequestParam(value = "ownerIdentity", required = false) MultipartFile ownerIdentity,
            @RequestParam(value = "otherProofs", required = false) MultipartFile[] otherProofs,
            @RequestParam("agreedToTerms") Boolean agreedToTerms) {
        
        // Parse location data from JSON
        String location = null;
        String place = null;
        String country = null;
        Double latitude = null;
        Double longitude = null;
        
        if (locationDataJson != null && !locationDataJson.isEmpty()) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                com.fasterxml.jackson.databind.JsonNode locationNode = mapper.readTree(locationDataJson);
                location = locationNode.has("location") ? locationNode.get("location").asText() : null;
                place = locationNode.has("place") ? locationNode.get("place").asText() : null;
                country = locationNode.has("country") ? locationNode.get("country").asText() : null;
                if (locationNode.has("latitude") && !locationNode.get("latitude").isNull()) {
                    latitude = locationNode.get("latitude").asDouble();
                }
                if (locationNode.has("longitude") && !locationNode.get("longitude").isNull()) {
                    longitude = locationNode.get("longitude").asDouble();
                }
            } catch (Exception e) {
                // Log error but continue
                System.err.println("Error parsing location data: " + e.getMessage());
            }
        }
        
        // Create DTO from request parameters
        VendorRegisterRequestDto requestDto = new VendorRegisterRequestDto();
        requestDto.setBusinessName(businessName);
        requestDto.setServiceCategory(serviceCategory);
        requestDto.setCustomService(customService);
        requestDto.setEmail(email);
        requestDto.setPhone(phone);
        requestDto.setLocation(location);
        requestDto.setPlace(place);
        requestDto.setCountry(country);
        requestDto.setLatitude(latitude);
        requestDto.setLongitude(longitude);
        requestDto.setDescription(description);
        requestDto.setBusinessLicense(businessLicense);
        requestDto.setOwnerIdentity(ownerIdentity);
        requestDto.setOtherProofs(otherProofs);
        requestDto.setAgreedToTerms(agreedToTerms);
        
        // Validate the request
        List<String> validationErrors = vendorRegistrationValidator.validate(requestDto);
        if (!validationErrors.isEmpty()) {
            return ResponseEntity.badRequest().body(
                VendorRegisterResponseDto.builder()
                    .success(false)
                    .message("Validation failed: " + String.join(", ", validationErrors))
                    .build()
            );
        }
        
        return ResponseEntity.ok(vendorRegistrationService.registerVendor(requestDto));
    }
    
    /**
     * Get list of valid service categories for vendor registration
     */
    @GetMapping("/vendor/service-categories")
    public ResponseEntity<List<String>> getServiceCategories() {
        List<String> categories = Arrays.stream(ServiceCategory.values())
            .map(ServiceCategory::getDisplayName)
            .collect(Collectors.toList());
        return ResponseEntity.ok(categories);
    }

    /**
     * Promote a user from USER role to ADMIN role
     * Only users with USER role can be promoted
     * Requires authentication token in header
     */
    @PostMapping("/admin/promote-user")
    public ResponseEntity<PromoteUserResponseDto> promoteUserToAdmin(@Valid @RequestBody PromoteUserRequestDto requestDto) {
        return ResponseEntity.ok(authService.promoteUserToAdmin(requestDto));
    }

}
