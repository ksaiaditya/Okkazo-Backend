package com.okkazo.authservice.services;

import com.okkazo.authservice.dtos.LoginRequestDto;
import com.okkazo.authservice.dtos.LoginResponseDto;
import com.okkazo.authservice.dtos.PromoteUserRequestDto;
import com.okkazo.authservice.dtos.PromoteUserResponseDto;
import com.okkazo.authservice.dtos.RegisterRequestDto;
import com.okkazo.authservice.dtos.RegisterResponseDto;
import com.okkazo.authservice.exceptions.*;
import com.okkazo.authservice.kafka.AuthEventProducer;
import com.okkazo.authservice.models.Auth;
import com.okkazo.authservice.models.EmailVerificationToken;
import com.okkazo.authservice.models.RefreshToken;
import com.okkazo.authservice.models.Role;
import com.okkazo.authservice.models.Status;
import com.okkazo.authservice.repositories.AuthRepository;
import com.okkazo.authservice.repositories.EmailVerificationTokenRepository;
import com.okkazo.authservice.utils.JwtUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {
    private final AuthRepository repository;
    private final PasswordEncoder passwordEncoder;
    private final ModelMapper modelMapper;
    private final AuthEventProducer authEvent;
    private final EmailVerificationTokenRepository emailVerificationTokenRepository;
    private final JwtUtil jwtUtil;
    private final RefreshTokenService refreshTokenService;
    
    @Value("${admin.promote-key}")
    private String adminPromoteKey;

    @Transactional
    public RegisterResponseDto register(RegisterRequestDto requestDto){
        Auth existingUser = repository.findByEmail(requestDto.email()).orElse(null);

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
        user.setEmail(requestDto.email());
        user.setHashedPassword(passwordEncoder.encode(requestDto.password()));
        user.setIsVerified(false);
        user.setStatus(Status.UNVERIFIED);
        user.setRole(Role.USER);

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
        Auth user = repository.findByEmail(requestDto.email())
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

        if (!passwordEncoder.matches(requestDto.password(), user.getHashedPassword())) {
            throw new InvalidCredentialsException("Invalid email or password");
        }

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
                "Login successful",
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
        return repository.findByEmail(email)
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
}
