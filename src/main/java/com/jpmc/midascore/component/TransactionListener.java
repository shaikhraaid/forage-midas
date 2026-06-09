package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    // Lets us find and save users in the database
    private final UserRepository userRepository;

    // Lets us save completed transactions in the database
    private final TransactionRepository transactionRepository;

    // Lets us call the external Incentive API
    private final RestTemplate restTemplate = new RestTemplate();

    // Reads incentive API URL from application.yml
    @Value("${general.incentive-api-url}")
    private String incentiveApiUrl;

    // Spring gives us the repositories automatically
    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    // Runs automatically whenever Kafka receives a Transaction
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {

        // Find sender by senderId
        UserRecord sender = userRepository.findById(transaction.getSenderId());

        // Find recipient by recipientId
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Stop if sender or recipient does not exist
        if (sender == null || recipient == null) {
            return;
        }

        // Stop if sender does not have enough money
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // Send transaction to Incentive API and receive incentive response
        Incentive incentive = restTemplate.postForObject(
                incentiveApiUrl,
                transaction,
                Incentive.class
        );

        // If API returns nothing, use 0 as incentive
        float incentiveAmount = incentive == null ? 0 : incentive.getAmount();

        // Subtract only transaction amount from sender
        sender.setBalance(sender.getBalance() - transaction.getAmount());

        // Add transaction amount plus incentive to recipient
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Save updated sender
        userRepository.save(sender);

        // Save updated recipient
        userRepository.save(recipient);

        // Save transaction record with incentive amount
        transactionRepository.save(
                new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount)
        );
    }
}