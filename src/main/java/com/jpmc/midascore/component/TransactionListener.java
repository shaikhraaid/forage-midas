package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) return;
        if (sender.getBalance() < transaction.getAmount()) return;

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = incentive == null ? 0 : (float) incentive.getAmount();

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        transactionRepository.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));
        userRepository.save(sender);
        userRepository.save(recipient);
    }
}