package br.com.atech.dao;

import org.springframework.data.jpa.repository.JpaRepository;

import br.com.atech.domain.Transport;

public interface DynamicReportsRepository extends JpaRepository<Transport, Long> {

}
