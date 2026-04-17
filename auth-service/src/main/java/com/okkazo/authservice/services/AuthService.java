package com.okkazo.authservice.services;

import com.okkazo.authservice.dtos.CheckEmailExistsResponseDto;
import com.okkazo.authservice.dtos.ChangePasswordRequestDto;
import com.okkazo.authservice.dtos.ChangePasswordResponseDto;
import com.okkazo.authservice.dtos.GoogleLoginRequestDto;
import com.okkazo.authservice.dtos.LoginRequestDto;
import com.okkazo.authservice.dtos.LoginResponseDto;
import com.okkazo.authservice.dtos.PromoteUserRequestDto;
import com.okkazo.authservice.dtos.PromoteUserResponseDto;
import com.okkazo.authservice.dtos.RegisterRequestDto;
import com.okkazo.authservice.dtos.RegisterResponseDto;
import com.okkazo.authservice.exceptions.*;
import com.okkazo.authservice.kafka.AuthEventProducer;
import com.okkazo.authservice.models.Auth;
import com.okkazo.authservice.models.AuthProvider;
import com.okkazo.authservice.models.EmailVerificationToken;
import com.okkazo.authservice.models.RefreshToken;
import com.okkazo.authservice.models.Role;
import com.okkazo.authservice.models.Status;
import com.okkazo.authservice.repositories.AuthRepository;
import com.okkazo.authservice.repositories.EmailVerificationTokenRepository;
import com.okkazo.authservice.utils.PasswordPolicy;
import com.okkazo.authservice.utils.JwtUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {
    private final AuthRepository repository;
    private final PasswordEncoder passwordEncoder;
    private final AuthEventProducer authEvent;
    private final EmailVerificationTokenRepository emailVerificationTokenRepository;
    private final JwtUtil jwtUtil;
    private final RefreshTokenService refreshTokenService;
                private final DisposableEmailDomainService disposableEmailDomainService;
        private final ObjectMapper objectMapper;
        private final HttpClient httpClient = HttpClient.newHttpClient();
    
    @Value("${admin.promote-key}")
    private String adminPromoteKey;

        @Value("${google.oauth.user-info-url:https://openidconnect.googleapis.com/v1/userinfo}")
        private String googleUserInfoUrl;

    @Transactional
    public RegisterResponseDto register(RegisterRequestDto requestDto){
                String normalizedEmail = normalizeEmail(requestDto.email());
                if (disposableEmailDomainService.isDisposableEmail(normalizedEmail)) {
                        throw new InvalidEmailDomainException("Temporary/disposable email addresses are not allowed. Please use a valid email.");
                }
                Auth existingUser = repository.findByEmailIgnoreCase(normalizedEmail).orElse(null);

        if (existingUser != null) {

            if (existingUser.getStatus() == Status.BLOCKED) {
                throw new AlreadyExistingException(
                        "Email already exists, please contact Okkazo team"
                );
            }

            if (!existingUser.getIsVerified()) {

                EmailVerificationToken latestToken =
                        emailVerificationTokenRepository
                                .findTopByUserOrderByCreatedAtDesc(existingUser)
                                .orElse(null);
                if (latestToken == null || latestToken.getExpiresAt().isBefore(LocalDateTime.now())) {
                    return resendVerification(existingUser);
                }

                throw new AlreadyExistingException(
                        "Email already exists. Please check your email for verification."
                );
            }
            throw new AlreadyExistingException(
                    "Email already exists, try logging in"
            );
        }


        Auth user = new Auth();
        user.setUsername(requestDto.username());
        user.setEmail(normalizedEmail);
        user.setHashedPassword(passwordEncoder.encode(requestDto.password()));
        user.setIsVerified(false);
        user.setStatus(Status.UNVERIFIED);
        user.setRole(Role.USER);
        user.setAuthProvider(AuthProvider.EMAIL);

        repository.save(user);

        String token = UUID.randomUUID().toString();

        EmailVerificationToken emailVerificationToken = new EmailVerificationToken();
        emailVerificationToken.setUser(user);
        emailVerificationToken.setHashedToken(passwordEncoder.encode(token));
        emailVerificationToken.setExpiresAt(LocalDateTime.now().plusMinutes(15));

        emailVerificationTokenRepository.save(emailVerificationToken);

        authEvent.userRegistered(
                user.getAuthId(),
                user.getEmail(),
                user.getUsername(),
                token);

        log.info("User registered successfully: {}", user.getEmail());

        return new RegisterResponseDto("User registered successfully, Please verify your email.", true);
    }

    private RegisterResponseDto resendVerification(Auth existingUser) {
        String rawToken = UUID.randomUUID().toString();
        EmailVerificationToken emailVerificationToken = new EmailVerificationToken();
        emailVerificationToken.setUser(existingUser);
        emailVerificationToken.setUsed(false);
        emailVerificationToken.setHashedToken(passwordEncoder.encode(rawToken));
        emailVerificationToken.setExpiresAt(LocalDateTime.now().plusMinutes(15));

        emailVerificationTokenRepository.save(emailVerificationToken);

        authEvent.userRegistered(
                existingUser.getAuthId(),
                existingUser.getEmail(),
                existingUser.getUsername(),
                rawToken);
        return new RegisterResponseDto("User registered successfully, Please verify your email.", true);
    }

    @Transactional
    public LoginResponseDto login(LoginRequestDto requestDto) {
                Auth user = repository.findByEmailIgnoreCase(normalizeEmail(requestDto.email()))
                .orElseThrow(() -> new InvalidCredentialsException("Invalid email or password"));

        if (user.getStatus() == Status.BLOCKED) {
            throw new AccountBlockedException("Your account has been blocked. Please contact support.");
        }

        // Check verification status BEFORE password check for unverified users
        if (!user.getIsVerified()) {
            // Different message for vendors (need to set password) vs regular users (need to verify email)
            if (user.getRole() == Role.VENDOR) {
                throw new EmailNotVerifiedException(
                        "Please set your password using the link sent to your email before logging in."
                );
            } else {
                throw new EmailNotVerifiedException(
                        "Please verify your email before logging in. Check your inbox for verification link."
                );
            }
        }

                if (user.getAuthProvider() == AuthProvider.SIGN_IN_WITH_GOOGLE) {
                        throw new InvalidCredentialsException("This account is linked to Google. Please sign in with Google.");
                }

                if (!passwordEncoder.matches(requestDto.password(), user.getHashedPassword())) {
            throw new InvalidCredentialsException("Invalid email or password");
        }

                return issueLoginResponse(user);
        }

        @Transactional
        public LoginResponseDto loginWithGoogle(GoogleLoginRequestDto requestDto) {
                JsonNode userInfo = fetchGoogleUserInfo(requestDto.accessToken());
                String email = normalizeEmail(userInfo.path("email").asText(""));
                String name = userInfo.path("name").asText("");
                boolean verified = userInfo.path("email_verified").asBoolean(false);

                if (email.isBlank() || !verified) {
                        throw new InvalidCredentialsException("Google account email is missing or not verified");
                }

                Auth user = repository.findByEmailIgnoreCase(email).orElse(null);

                if (user != null) {
                        if (user.getStatus() == Status.BLOCKED) {
                                throw new AccountBlockedException("Your account has been blocked. Please contact support.");
                        }

                        AuthProvider currentProvider = user.getAuthProvider() == null
                                        ? AuthProvider.EMAIL
                                        : user.getAuthProvider();
                        if (currentProvider == AuthProvider.EMAIL) {
                                user.setAuthProvider(AuthProvider.BOTH);
                        } else if (currentProvider == AuthProvider.SIGN_IN_WITH_GOOGLE) {
                                user.setAuthProvider(AuthProvider.SIGN_IN_WITH_GOOGLE);
                        } else {
                                user.setAuthProvider(AuthProvider.BOTH);
                        }

                        if (!Boolean.TRUE.equals(user.getIsVerified())) {
                                user.setIsVerified(true);
                        }
                        if (user.getStatus() == Status.UNVERIFIED) {
                                user.setStatus(Status.ACTIVE);
                        }

                        repository.save(user);
                        return issueLoginResponse(user);
                }

                Auth newUser = new Auth();
                newUser.setUsername(generateUsernameFromGoogle(name, email));
                newUser.setEmail(email);
                newUser.setHashedPassword(passwordEncoder.encode(UUID.randomUUID().toString()));
                newUser.setIsVerified(true);
                newUser.setStatus(Status.ACTIVE);
                newUser.setRole(Role.USER);
                newUser.setAuthProvider(AuthProvider.SIGN_IN_WITH_GOOGLE);

                repository.save(newUser);
                authEvent.userGoogleRegistered(newUser.getAuthId(), newUser.getEmail(), newUser.getUsername());

                log.info("User signed up via Google successfully: {}", newUser.getEmail());
                return issueLoginResponse(newUser);
        }

        private LoginResponseDto issueLoginResponse(Auth user) {

        String accessToken = jwtUtil.generateAccessToken(
                user.getAuthId(),
                user.getEmail(),
                user.getUsername(),
                user.getRole().name()
        );

        String rawRefreshToken = UUID.randomUUID().toString();
        RefreshToken refreshToken = refreshTokenService.createRefreshToken(user, rawRefreshToken);

        String refreshTokenJwt = jwtUtil.generateRefreshToken(
                user.getAuthId(),
                refreshToken.getId()
        );

        authEvent.userLoginEvent(user.getAuthId(), user.getEmail());

        log.info("User logged in successfully: {}", user.getEmail());

        return new LoginResponseDto(
                accessToken,
                refreshTokenJwt,
                user.getRole().name(),
                user.getAuthProvider() == null ? AuthProvider.EMAIL.name() : user.getAuthProvider().name(),
                "Login successful",
                true
        );
    }

        private JsonNode fetchGoogleUserInfo(String accessToken) {
                try {
                        HttpRequest request = HttpRequest.newBuilder()
                                        .uri(URI.create(googleUserInfoUrl))
                                        .header("Authorization", "Bearer " + accessToken)
                                        .GET()
                                        .build();

                        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                        if (response.statusCode() < 200 || response.statusCode() >= 300) {
                                throw new InvalidCredentialsException("Invalid Google token");
                        }

                        return objectMapper.readTree(response.body());
                } catch (InvalidCredentialsException ex) {
                        throw ex;
                } catch (Exception ex) {
                        log.error("Failed to validate Google token", ex);
                        throw new InvalidCredentialsException("Google sign-in failed");
                }
        }

        private String generateUsernameFromGoogle(String name, String email) {
                String preferred = (name == null || name.isBlank()) ? email.split("@")[0] : name;
                String base = preferred.toLowerCase().replaceAll("[^a-z0-9_]", "_").replaceAll("_+", "_");
                if (base.length() < 3) {
                        base = "user_" + UUID.randomUUID().toString().substring(0, 8);
                }
                String candidate = base;
                int suffix = 1;
                while (repository.existsByUsername(candidate)) {
                        candidate = base + "_" + suffix;
                        suffix++;
                }
                return candidate;
        }

        private String normalizeEmail(String email) {
                return email == null ? "" : email.trim().toLowerCase();
        }

        @Transactional
        public ChangePasswordResponseDto changePassword(UUID authId, ChangePasswordRequestDto requestDto) {
                if (authId == null) {
                        throw new InvalidTokenException("Authentication context is missing");
                }

                Auth user = repository.findById(authId)
                                .orElseThrow(() -> new UserNotFoundException("User not found"));

                if (user.getStatus() == Status.BLOCKED) {
                        throw new AccountBlockedException("Your account has been blocked. Please contact support.");
                }

                if (user.getAuthProvider() == AuthProvider.SIGN_IN_WITH_GOOGLE) {
                        throw new IllegalArgumentException("Password change is not available for Google-only accounts");
                }

                String currentPassword = requestDto.currentPassword();
                String newPassword = requestDto.newPassword();
                String confirmPassword = requestDto.confirmPassword();

                if (!passwordEncoder.matches(currentPassword, user.getHashedPassword())) {
                        throw new InvalidCredentialsException("Current password is incorrect");
                }

                if (!PasswordPolicy.isStrong(newPassword)) {
                        throw new IllegalArgumentException(PasswordPolicy.MESSAGE);
                }

                if (!newPassword.equals(confirmPassword)) {
                        throw new IllegalArgumentException("New password and confirm password do not match");
                }

                if (passwordEncoder.matches(newPassword, user.getHashedPassword())) {
                        throw new IllegalArgumentException("New password must be different from current password");
                }

                user.setHashedPassword(passwordEncoder.encode(newPassword));
                repository.save(user);

                authEvent.passwordChanged(
                                user.getAuthId(),
                                user.getEmail(),
                                user.getUsername(),
                                LocalDateTime.now()
                );

                log.info("Password changed successfully for user: {}", user.getEmail());

                return new ChangePasswordResponseDto(
                                "Password changed successfully",
                                true
                );
        }

    @Transactional
    public PromoteUserResponseDto promoteUserToAdmin(PromoteUserRequestDto requestDto) {
        // Validate security key
        if (!adminPromoteKey.equals(requestDto.getKey())) {
            log.warn("Invalid security key provided for promote user request: {}", requestDto.getEmail());
            return PromoteUserResponseDto.builder()
                    .success(false)
                    .message("Invalid security key")
                    .email(requestDto.getEmail())
                    .previousRole(null)
                    .newRole(null)
                    .build();
        }
        
        Auth user = repository.findByEmail(requestDto.getEmail())
                .orElseThrow(() -> new UserNotFoundException("User not found with email: " + requestDto.getEmail()));

        // Check if user is already an ADMIN
        if (user.getRole() == Role.ADMIN) {
            return PromoteUserResponseDto.builder()
                    .success(false)
                    .message("User is already an ADMIN")
                    .email(user.getEmail())
                    .previousRole(Role.ADMIN.name())
                    .newRole(Role.ADMIN.name())
                    .build();
        }

        // Only allow promotion if current role is USER
        if (user.getRole() != Role.USER) {
            return PromoteUserResponseDto.builder()
                    .success(false)
                    .message("Only users with USER role can be promoted to ADMIN. Current role: " + user.getRole().name())
                    .email(user.getEmail())
                    .previousRole(user.getRole().name())
                    .newRole(user.getRole().name())
                    .build();
        }

        String previousRole = user.getRole().name();
        user.setRole(Role.ADMIN);
        repository.save(user);

        // Send event to user-service to update role there
        authEvent.userRoleChanged(user.getAuthId(), user.getEmail(), previousRole, Role.ADMIN.name());

        log.info("User promoted to ADMIN: {} (previous role: {})", user.getEmail(), previousRole);

        return PromoteUserResponseDto.builder()
                .success(true)
                .message("User successfully promoted to ADMIN")
                .email(user.getEmail())
                .previousRole(previousRole)
                .newRole(Role.ADMIN.name())
                .build();
    }

    public CheckEmailExistsResponseDto checkEmailExists(String email) {
        return repository.findByEmailIgnoreCase(normalizeEmail(email))
                .map(user -> new CheckEmailExistsResponseDto(
                        true,
                        user.getRole().name(),
                        "Email exists"
                ))
                .orElse(new CheckEmailExistsResponseDto(
                        false,
                        null,
                        "Email does not exist"
                ));
    }

    @Transactional(readOnly = true)
    public Map<String, String> getAccountStatuses(List<String> authIds) {
        Map<String, String> statuses = new HashMap<>();

        if (authIds == null || authIds.isEmpty()) {
            return statuses;
        }

        List<UUID> validIds = authIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .map(id -> {
                    try {
                        return UUID.fromString(id);
                    } catch (IllegalArgumentException ex) {
                        return null;
                    }
                })
                .filter(id -> id != null)
                .toList();

        if (validIds.isEmpty()) {
            return statuses;
        }

        List<Auth> accounts = repository.findByAuthIdIn(validIds);
        for (Auth account : accounts) {
            statuses.put(
                    String.valueOf(account.getAuthId()),
                    account.getStatus() == null ? "UNVERIFIED" : account.getStatus().name()
            );
        }

        for (UUID id : validIds) {
            statuses.putIfAbsent(String.valueOf(id), "UNKNOWN");
        }

        return statuses;
    }

    @Transactional
    public void blockTeamAccessAccount(String authId, String changedBy) {
        if (authId == null || authId.isBlank()) {
            throw new InvalidTokenException("Auth ID is required");
        }

        UUID parsedAuthId;
        try {
            parsedAuthId = UUID.fromString(authId);
        } catch (IllegalArgumentException ex) {
            throw new InvalidTokenException("Invalid auth ID");
        }

        Auth user = repository.findById(parsedAuthId)
                .orElseThrow(() -> new UserNotFoundException("User not found"));

        user.setStatus(Status.BLOCKED);
        repository.save(user);
        refreshTokenService.revokeAllUserTokens(user);

        log.info("Account blocked via internal team access endpoint: authId={}, changedBy={}", authId, changedBy);
    }

    @Transactional
    public void unblockTeamAccessAccount(String authId, String changedBy) {
        if (authId == null || authId.isBlank()) {
            throw new InvalidTokenException("Auth ID is required");
        }

        UUID parsedAuthId;
        try {
            parsedAuthId = UUID.fromString(authId);
        } catch (IllegalArgumentException ex) {
            throw new InvalidTokenException("Invalid auth ID");
        }

        Auth user = repository.findById(parsedAuthId)
                .orElseThrow(() -> new UserNotFoundException("User not found"));

        user.setStatus(Status.ACTIVE);
        repository.save(user);

        log.info("Account unblocked via internal team access endpoint: authId={}, changedBy={}", authId, changedBy);
    }

        @Transactional(readOnly = true)
        public Map<String, Object> getAdminPlatformUsers(int page, int limit, String role, String search) {
                int safePage = Math.max(page, 1);
                int safeLimit = Math.min(Math.max(limit, 1), 100);

                Role roleFilter = null;
                if (role != null && !role.isBlank()) {
                        try {
                                roleFilter = Role.valueOf(role.trim().toUpperCase());
                        } catch (IllegalArgumentException ignored) {
                                roleFilter = null;
                        }
                }

                String normalizedSearch = search == null ? "" : search.trim();

                Pageable pageable = PageRequest.of(
                                safePage - 1,
                                safeLimit,
                                Sort.by(Sort.Direction.DESC, "createdAt")
                );

                Page<Auth> authPage = repository.findPlatformUsers(roleFilter, normalizedSearch, pageable);

                List<Map<String, Object>> users = authPage.getContent().stream().map(auth -> {
                        Map<String, Object> item = new HashMap<>();
                        item.put("authId", auth.getAuthId());
                        item.put("email", auth.getEmail());
                        item.put("name", auth.getUsername());
                        item.put("username", auth.getUsername());
                        item.put("role", auth.getRole() == null ? Role.USER.name() : auth.getRole().name());
                        item.put("accountStatus", auth.getStatus() == null ? Status.UNVERIFIED.name() : auth.getStatus().name());
                        item.put("isVerified", Boolean.TRUE.equals(auth.getIsVerified()));
                        item.put("createdAt", auth.getCreatedAt());
                        item.put("updatedAt", auth.getUpdatedAt());
                        return item;
                }).toList();

                Map<String, Long> byRole = new HashMap<>();
                for (Role enumRole : Role.values()) {
                        byRole.put(enumRole.name(), repository.countByRole(enumRole));
                }

                long totalUsers = repository.count();
                long activeUsers = repository.countByStatus(Status.ACTIVE);

                Map<String, Object> stats = new HashMap<>();
                stats.put("totalUsers", totalUsers);
                stats.put("activeUsers", activeUsers);
                stats.put("byRole", byRole);

                Map<String, Object> pagination = new HashMap<>();
                pagination.put("currentPage", safePage);
                pagination.put("totalPages", authPage.getTotalPages());
                pagination.put("totalUsers", authPage.getTotalElements());
                pagination.put("limit", safeLimit);

                Map<String, Object> result = new HashMap<>();
                result.put("users", users);
                result.put("pagination", pagination);
                result.put("stats", stats);

                return result;
        }
}
