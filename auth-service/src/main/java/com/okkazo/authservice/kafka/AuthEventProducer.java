package com.okkazo.authservice.kafka;

import com.okkazo.authservice.dtos.EmailVerificationResendEvent;
import com.okkazo.authservice.dtos.PasswordResetEvent;
import com.okkazo.authservice.dtos.UserLoginEvent;
import com.okkazo.authservice.dtos.UserRegistrationEvent;
import com.okkazo.authservice.dtos.UserGoogleRegisteredEvent;
import com.okkazo.authservice.dtos.UserRoleChangedEvent;
import com.okkazo.authservice.dtos.UserPasswordChangedEvent;
import com.okkazo.authservice.dtos.ManagerAccountCreatedEvent;
import com.okkazo.authservice.dtos.VendorAccountCreatedEvent;
import com.okkazo.authservice.dtos.VendorRegistrationEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class AuthEventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topicName;

    public void userRegistered(UUID authId, String email, String username, String verificationToken){
        UserRegistrationEvent event = new UserRegistrationEvent(
                "USER_REGISTERED",
                authId,
                email,
                username,
                verificationToken
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void passwordResetRequested(UUID authId, String email, String resetToken){
        PasswordResetEvent event = new PasswordResetEvent(
                "PASSWORD_RESET_REQUESTED",
                authId,
                email,
                resetToken
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void emailVerificationResend(UUID authId, String email, String verificationToken){
        EmailVerificationResendEvent event = new EmailVerificationResendEvent(
                "EMAIL_VERIFICATION_RESEND",
                authId,
                email,
                verificationToken
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void userLoginEvent(UUID authId, String email){
        UserLoginEvent event = new UserLoginEvent(
                "USER_LOGIN",
                authId,
                email,
                LocalDateTime.now()
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void userGoogleRegistered(UUID authId, String email, String username) {
        UserGoogleRegisteredEvent event = new UserGoogleRegisteredEvent(
                "USER_GOOGLE_REGISTERED",
                authId,
                email,
                username
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void passwordChanged(UUID authId, String email, String username, LocalDateTime changedAt) {
        UserPasswordChangedEvent event = new UserPasswordChangedEvent(
                "PASSWORD_CHANGED",
                authId,
                email,
                username,
                changedAt == null ? LocalDateTime.now() : changedAt
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }
    
    public void vendorRegistrationSubmitted(VendorRegistrationEvent event){
        kafkaTemplate.send("vendor_events", event.getApplicationId(), event);
    }
    
    public void vendorAccountCreated(UUID authId, String email, String passwordResetToken, String businessName, String applicationId){
        VendorAccountCreatedEvent event = new VendorAccountCreatedEvent(
                "VENDOR_ACCOUNT_CREATED",
                authId,
                email,
                passwordResetToken,
                businessName,
                applicationId
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void userRoleChanged(UUID authId, String email, String previousRole, String newRole){
        UserRoleChangedEvent event = new UserRoleChangedEvent(
                "USER_ROLE_CHANGED",
                authId,
                email,
                previousRole,
                newRole,
                LocalDateTime.now()
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }

    public void managerAccountCreated(UUID authId, String email, String passwordResetToken, String name, String department, String assignedRole){
        ManagerAccountCreatedEvent event = new ManagerAccountCreatedEvent(
                "MANAGER_ACCOUNT_CREATED",
                authId,
                email,
                passwordResetToken,
                name,
                department,
                assignedRole
        );
        kafkaTemplate.send(topicName, authId.toString(), event);
    }
}
